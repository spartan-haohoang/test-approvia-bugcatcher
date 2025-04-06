package com.ursalive.manager.shop

import aws.smithy.kotlin.runtime.content.BigDecimal
import com.ursalive.api.error.ShopError
import com.ursalive.cache.redis.custom.ShopCache
import com.ursalive.client.common.ShopPageableResponse
import com.ursalive.client.request.shop.ShopDetachPaymentMethodRequest
import com.ursalive.client.request.shop.ShopPaymentCalculateTaxRequest
import com.ursalive.client.request.shop.ShopPaymentCreateIntentRequest
import com.ursalive.client.request.shop.ShopPaymentCreatePayPalOrderRequest
import com.ursalive.client.request.shop.ShopPaymentPayRequest
import com.ursalive.client.response.shop.ShopDetachPaymentMethodResponse
import com.ursalive.client.response.shop.ShopMyCard
import com.ursalive.client.response.shop.ShopMyPaymentMethodResponse
import com.ursalive.client.response.shop.ShopPaymentCalculateTaxResponse
import com.ursalive.client.response.shop.ShopPaymentCreateIntentResponse
import com.ursalive.client.response.shop.ShopPaymentCreatePayPalOrderResponse
import com.ursalive.client.response.shop.ShopPaymentPayResponse
import com.ursalive.client.response.shop.ShopPaymentSetupIntentResponse
import com.ursalive.communication.email.EmailService
import com.ursalive.communication.email.ShopEventRecapEmailRenderer
import com.ursalive.communication.email.ShopPurchasedOrderEmailRenderer
import com.ursalive.communication.model.response.ShopSocialLinks
import com.ursalive.database.runtime.DatabaseContext
import com.ursalive.entity.constant.Currency
import com.ursalive.entity.constant.PaymentGateway
import com.ursalive.entity.constant.PaymentMethod
import com.ursalive.entity.constant.ShopOrderStatus
import com.ursalive.entity.constant.ShopTransactionStatus
import com.ursalive.entity.constant.ShopTransactionType
import com.ursalive.entity.shop.ShopEventEntity
import com.ursalive.entity.shop.ShopOrderEntity
import com.ursalive.entity.shop.ShopOrderProductEntity
import com.ursalive.entity.shop.ShopPaymentEntity
import com.ursalive.entity.shop.ShopPaymentMethodEntity
import com.ursalive.entity.shop.ShopProductTaxEntity
import com.ursalive.entity.shop.ShopShippingAddressEntity
import com.ursalive.entity.shop.ShopShippingDetailEntity
import com.ursalive.entity.shop.ShopTransactionEntity
import com.ursalive.entity.shop.ShopUserEntity
import com.ursalive.entity.view.shop.ShopProductView
import com.ursalive.logging.Logging
import com.ursalive.logging.warn
import com.ursalive.manager.payment.PayPalPaymentStatus
import com.ursalive.manager.shop.constant.ShopConstant
import com.ursalive.manager.shop.constant.StripePaymentStatus
import com.ursalive.manager.shop.hubspot.ShopHubspotManager
import com.ursalive.repository.shop.ShopEventReportRepository
import com.ursalive.repository.shop.ShopEventRepository
import com.ursalive.repository.shop.ShopOrderProductRepository
import com.ursalive.repository.shop.ShopOrderRepository
import com.ursalive.repository.shop.ShopPaymentMethodRepository
import com.ursalive.repository.shop.ShopPaymentRepository
import com.ursalive.repository.shop.ShopProductPictureRepository
import com.ursalive.repository.shop.ShopProductRepository
import com.ursalive.repository.shop.ShopProductTaxRepository
import com.ursalive.repository.shop.ShopShippingAddressRepository
import com.ursalive.repository.shop.ShopShippingCountryRepository
import com.ursalive.repository.shop.ShopShippingDetailRepository
import com.ursalive.repository.shop.ShopTransactionRepository
import com.ursalive.repository.shop.ShopUserRepository
import com.ursalive.runtime.config.AppConfiguration
import com.ursalive.web.socket.response.SoldProductResponse
import java.math.RoundingMode
import java.util.UUID
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import org.jetbrains.exposed.sql.transactions.transaction

interface ShopPaymentManager {
  fun setupIntent(userId: UUID): ShopPaymentSetupIntentResponse
  fun pay(buyerId: UUID, request: ShopPaymentPayRequest): ShopPaymentPayResponse
  fun retrievePaymentMethods(userId: UUID): ShopPageableResponse<ShopMyPaymentMethodResponse>
  fun detachPaymentMethod(request: ShopDetachPaymentMethodRequest): ShopDetachPaymentMethodResponse
  fun createIntent(buyerId: UUID, request: ShopPaymentCreateIntentRequest): ShopPaymentCreateIntentResponse
  fun calculateTax(buyerId: UUID, request: ShopPaymentCalculateTaxRequest): ShopPaymentCalculateTaxResponse
  fun createPayPalOrder(
    buyerId: UUID,
    request: ShopPaymentCreatePayPalOrderRequest,
  ): ShopPaymentCreatePayPalOrderResponse
}

class DefaultShopPaymentManager(
  private val shopCache: ShopCache,
  private val emailService: EmailService,
  private val databaseContext: DatabaseContext,
  private val appConfiguration: AppConfiguration,
  private val shopStripeManager: ShopStripeManager,
  private val shopPayPalManager: ShopPayPalManager,
  private val shopSocketManager: ShopSocketManager,
  private val shopUserRepository: ShopUserRepository,
  private val shopEventRepository: ShopEventRepository,
  private val shopOrderRepository: ShopOrderRepository,
  private val shopProductRepository: ShopProductRepository,
  private val shopPaymentRepository: ShopPaymentRepository,
  private val shopProductTaxRepository: ShopProductTaxRepository,
  private val shopEventReportRepository: ShopEventReportRepository,
  private val shopTransactionRepository: ShopTransactionRepository,
  private val shopOrderProductRepository: ShopOrderProductRepository,
  private val shopPaymentMethodRepository: ShopPaymentMethodRepository,
  private val shopShippingDetailRepository: ShopShippingDetailRepository,
  private val shopProductPictureRepository: ShopProductPictureRepository,
  private val shopShippingAddressRepository: ShopShippingAddressRepository,
  private val shopShippingCountryRepository: ShopShippingCountryRepository,
  private val shopHubspotManager: ShopHubspotManager,
  private val coroutineScope: CoroutineScope
) : ShopPaymentManager, Logging {

  override fun setupIntent(userId: UUID): ShopPaymentSetupIntentResponse {
    val user = shopUserRepository.findById(id = userId) ?: throw ShopError.USER_NOT_FOUND.asException()

    val setupIntent = user.customerId?.let {
      shopStripeManager.setupIntent(customerId = it)
    } ?: throw ShopError.STRIPE_CUSTOMER_NOT_FOUND.asException()

    return ShopPaymentSetupIntentResponse(clientSecret = setupIntent.clientSecret, nextAction = setupIntent.nextAction)
  }

  override fun pay(buyerId: UUID, request: ShopPaymentPayRequest): ShopPaymentPayResponse {
    val buyer = shopUserRepository.findById(id = buyerId) ?: throw ShopError.USER_NOT_FOUND.asException()

    val product = shopProductRepository.findById(
      id = request.productId
    ) ?: throw ShopError.PRODUCT_NOT_FOUND.asException()

    if (product.isSoldOut) {
      throw ShopError.PRODUCT_IS_SOLD_OUT.asException()
    }

    val paymentPayLock = shopCache.increasePaymentPayLock(key = product.id.toString())
    val remainQuantity = product.stock - product.soldQuantity

    if (remainQuantity < 0) {
      shopCache.decreasePaymentPayLock(key = product.id.toString())
      throw ShopError.PRODUCT_TEMPORARILY_OUT_OF_STOCK.asException()
    }
    if (remainQuantity < paymentPayLock) {
      shopCache.decreasePaymentPayLock(key = product.id.toString())
      throw ShopError.ORDER_QUANTITY_EXCEEDS_AVAILABLE_INVENTORY.asException()
    }

    val shippingAddress: ShopShippingAddressEntity
    val productTax: ShopProductTaxEntity
    val event: ShopEventEntity?
    try {
      shippingAddress = shopShippingAddressRepository.findDefaultByBuyerId(
        buyerId = buyer.id
      ) ?: throw ShopError.SHIPPING_ADDRESS_NOT_FOUND.asException()

      productTax = shopProductTaxRepository.findBy(
        buyerId = buyer.id,
        productId = product.id,
        shippingAddressId = shippingAddress.id,
      ) ?: throw ShopError.PRODUCT_TAX_NOT_FOUND.asException()

      event = request.eventId?.let {
        shopEventRepository.findById(id = it) ?: throw ShopError.EVENT_NOT_FOUND.asException()
      }
    } catch (e: Exception) {
      shopCache.decreasePaymentPayLock(key = product.id.toString())
      throw e
    }

    return runCatching {
      val (purchasedProduct, purchasedOrder) = when (request.paymentMethod) {
        PaymentMethod.CARD -> {
          val total = (product.price * ShopConstant.ORDER_QUANTITY_DEFAULT) + productTax.taxAmount + product.shippingFee
          val transactionFee = calculateTransactionFee(amount = total, paymentMethod = request.paymentMethod)
          val payableAmount = total + transactionFee

          val paymentMethodId = request.paymentSource.paymentMethodId.let {
            it ?: throw ShopError.PAYMENT_METHOD_ID_REQUIRED.asException()
          }

          val paymentIntent = shopStripeManager.createIntent(
            amount = payableAmount,
            customerId = buyer.customerId,
            paymentMethodId = paymentMethodId
          )

          purchase(
            buyer = buyer,
            event = event,
            product = product,
            extPaymentId = paymentIntent.id,
            taxAmount = productTax.taxAmount,
            shippingAddress = shippingAddress,
            orderStatus = ShopOrderStatus.SOLD,
            paymentMethod = request.paymentMethod,
            extPaymentStatus = paymentIntent.status,
            shippingFee = product.shippingFee,
            extPaymentMethodId = paymentIntent.paymentMethod,
            orderQuantity = ShopConstant.ORDER_QUANTITY_DEFAULT.toInt()
          )
        }

        PaymentMethod.PAYPAL -> {
          val orderId = request.paymentSource.orderId.let {
            it ?: throw ShopError.ORDER_ID_REQUIRED.asException()
          }

          val order = shopPayPalManager.retrieveOrder(orderId = orderId)
          val authorizationId = order.purchaseUnits()[0].payments().authorizations()[0].id()
          val capturedAuthorizedPayment = shopPayPalManager.captureAuthorizedPayment(authorizationId = authorizationId)
          val status = PayPalPaymentStatus.valueOf(capturedAuthorizedPayment.status())
          if (status != PayPalPaymentStatus.COMPLETED) {
            throw ShopError.PAY_FAILED.asException("Failed to pay with OrderID=[${order.id()}] from PayPal")
          }

          purchase(
            buyer = buyer,
            event = event,
            product = product,
            taxAmount = productTax.taxAmount,
            shippingAddress = shippingAddress,
            orderStatus = ShopOrderStatus.SOLD,
            paymentMethod = request.paymentMethod,
            shippingFee = product.shippingFee,
            extPaymentId = capturedAuthorizedPayment.id(),
            extPaymentStatus = capturedAuthorizedPayment.status(),
            orderQuantity = ShopConstant.ORDER_QUANTITY_DEFAULT.toInt()
          )
        }

        PaymentMethod.APPLE_PAY, PaymentMethod.GOOGLE_PAY -> {
          val paymentIntentId = request.paymentSource.paymentIntentId.let {
            it ?: throw ShopError.PAYMENT_INTENT_ID_REQUIRED.asException()
          }

          val paymentIntent = shopStripeManager.retrievePaymentIntent(paymentIntentId = paymentIntentId)
          val capturedPaymentIntent = shopStripeManager.capturePaymentIntent(paymentIntentId = paymentIntent.id)
          val status = StripePaymentStatus.fromValue(value = capturedPaymentIntent.status)
          if (status != StripePaymentStatus.SUCCEEDED) {
            throw ShopError.PAY_FAILED.asException("Failed to pay with PaymentIntentID=[${paymentIntent.id}] from Stripe")
          }

          purchase(
            buyer = buyer,
            event = event,
            product = product,
            taxAmount = productTax.taxAmount,
            shippingAddress = shippingAddress,
            orderStatus = ShopOrderStatus.SOLD,
            paymentMethod = request.paymentMethod,
            shippingFee = product.shippingFee,
            extPaymentId = capturedPaymentIntent.id,
            extPaymentStatus = capturedPaymentIntent.status,
            extPaymentMethodId = capturedPaymentIntent.paymentMethod,
            orderQuantity = ShopConstant.ORDER_QUANTITY_DEFAULT.toInt()
          )
        }
      }

      val productPicture = shopProductPictureRepository.findDefaultByProductId(productId = purchasedProduct.id)

      shopSocketManager.emit(
        payload = SoldProductResponse(
          sellerId = purchasedProduct.sellerId,
          buyerId = buyer.id,
          buyerName = buyer.name,
          buyerUsername = buyer.username,
          productId = purchasedProduct.id,
          productStock = purchasedProduct.stock,
          productName = purchasedProduct.name,
          productSoldQuantity = purchasedProduct.soldQuantity,
          productPrice = purchasedProduct.price,
          productCurrency = purchasedProduct.currency,
          productItemNumber = purchasedProduct.itemNumber,
          productShippingFee = purchasedProduct.shippingFee,
          productDescription = purchasedProduct.description,
          productPicturePath = productPicture?.path,
          isSoldOut = purchasedProduct.isSoldOut,
          eventId = event?.id,
          hostId = event?.sellerId
        )
      )

      runBlocking {
        val seller = shopUserRepository.findById(
          id = purchasedProduct.sellerId
        ) ?: throw ShopError.USER_NOT_FOUND.asException()

        val orderProduct = shopOrderProductRepository.findByOrderIdAndProductId(
          orderId = purchasedOrder.id,
          productId = purchasedProduct.id
        ) ?: throw ShopError.ORDER_PRODUCT_NOT_FOUND.asException()

        emailService.send(
          fromEmail = appConfiguration.shop.email.noReplyEmail,
          toEmail = buyer.email,
          renderer = ShopPurchasedOrderEmailRenderer(
            buyer = buyer,
            seller = seller,
            order = purchasedOrder,
            product = purchasedProduct,
            orderProduct = orderProduct,
            productPicture = productPicture,
            cdnHost = appConfiguration.cdn.host,
            socialLinks = ShopSocialLinks(
              web = appConfiguration.shop.socialLinks.web,
              tiktok = appConfiguration.shop.socialLinks.tiktok,
              xSocial = appConfiguration.shop.socialLinks.xSocial,
              facebook = appConfiguration.shop.socialLinks.facebook,
              linkedIn = appConfiguration.shop.socialLinks.linkedIn,
              instagram = appConfiguration.shop.socialLinks.instagram,
              privacyPolicy = appConfiguration.shop.socialLinks.privacyPolicy
            )
          )
        )
        if (request.eventId == null) {
          val soldProducts = shopProductRepository.findProductPerOrders(orderIds = listOf(purchasedOrder.id))
          emailService.send(
            fromEmail = appConfiguration.shop.email.noReplyEmail,
            toEmail = seller.email,
            renderer = ShopEventRecapEmailRenderer(
              webHost = appConfiguration.shop.web.host,
              cdnHost = appConfiguration.shop.aws.cdn.host,
              socialLinks = ShopSocialLinks(
                web = appConfiguration.shop.socialLinks.web,
                tiktok = appConfiguration.shop.socialLinks.tiktok,
                xSocial = appConfiguration.shop.socialLinks.xSocial,
                facebook = appConfiguration.shop.socialLinks.facebook,
                linkedIn = appConfiguration.shop.socialLinks.linkedIn,
                instagram = appConfiguration.shop.socialLinks.instagram,
                privacyPolicy = appConfiguration.shop.socialLinks.privacyPolicy
              ),
              sellerUsername = seller.username,
              sellerFirstname = seller.firstName,
              soldProducts = soldProducts,
            )
          )
        }
      }

      ShopPaymentPayResponse(orderId = purchasedOrder.id)
    }.getOrElse {
      purchase(
        buyer = buyer,
        event = event,
        product = product,
        cancelledReason = it.message,
        taxAmount = productTax.taxAmount,
        shippingAddress = shippingAddress,
        paymentMethod = request.paymentMethod,
        orderStatus = ShopOrderStatus.CANCELLED,
        shippingFee = product.shippingFee,
        orderQuantity = ShopConstant.ORDER_QUANTITY_DEFAULT.toInt(),
        extPaymentMethodId = if (request.paymentMethod == PaymentMethod.CARD) {
          request.paymentSource.paymentMethodId
        } else {
          null
        }
      )

      shopCache.decreasePaymentPayLock(key = product.id.toString())
      warn(it) {
        "Failed to pay via ${request.paymentMethod} due to=${it.message}"
      }
      throw it
    }.also {
      shopCache.decreasePaymentPayLock(key = product.id.toString())
    }
  }

  override fun retrievePaymentMethods(userId: UUID): ShopPageableResponse<ShopMyPaymentMethodResponse> {
    val user = shopUserRepository.findById(id = userId) ?: throw ShopError.USER_NOT_FOUND.asException()

    val paymentMethodCollection = user.customerId?.let {
      shopStripeManager.retrievePaymentMethods(customerId = it)
    } ?: throw ShopError.STRIPE_CUSTOMER_NOT_FOUND.asException()

    return ShopPageableResponse(
      totalCount = paymentMethodCollection.data.size,
      data = paymentMethodCollection.data.map {
        val card = it.card
        ShopMyPaymentMethodResponse(
          id = it.id,
          ShopMyCard(
            last4 = card.last4,
            brand = card.brand,
            country = card.country,
            issuer = card.issuer,
            expMonth = card.expMonth,
            expYear = card.expYear,
            fingerprint = card.fingerprint,
            iin = card.iin,
            funding = card.funding
          )
        )
      }
    )
  }

  override fun detachPaymentMethod(request: ShopDetachPaymentMethodRequest): ShopDetachPaymentMethodResponse {
    val paymentMethod = shopStripeManager.detachPaymentMethod(paymentMethodId = request.paymentMethodId)
    return ShopDetachPaymentMethodResponse(paymentMethodId = paymentMethod.id)
  }

  override fun createIntent(buyerId: UUID, request: ShopPaymentCreateIntentRequest): ShopPaymentCreateIntentResponse {
    val buyer = shopUserRepository.findById(id = buyerId) ?: throw ShopError.USER_NOT_FOUND.asException()

    val shippingAddress = shopShippingAddressRepository.findDefaultByBuyerId(
      buyerId = buyer.id
    ) ?: throw ShopError.SHIPPING_ADDRESS_NOT_FOUND.asException()

    val product = shopProductRepository.findById(
      id = request.productId
    ) ?: throw ShopError.PRODUCT_NOT_FOUND.asException()

    if (product.isSoldOut) {
      throw ShopError.PRODUCT_IS_SOLD_OUT.asException()
    }

    val productTax = shopProductTaxRepository.findBy(
      buyerId = buyer.id,
      productId = product.id,
      shippingAddressId = shippingAddress.id,
    ) ?: throw ShopError.PRODUCT_TAX_NOT_FOUND.asException()

    val total = (product.price * ShopConstant.ORDER_QUANTITY_DEFAULT) + productTax.taxAmount + product.shippingFee
    val transactionFee = calculateTransactionFee(amount = total, paymentMethod = PaymentMethod.CARD)
    val payableAmount = total + transactionFee

    val paymentIntent = shopStripeManager.createIntent(
      amount = payableAmount,
      paymentMethodType = request.paymentMethodType
    )

    return ShopPaymentCreateIntentResponse(
      clientSecret = paymentIntent.clientSecret,
      nextAction = paymentIntent.nextAction,
      currency = product.currency,
      totalAmount = payableAmount,
      taxAmount = productTax.taxAmount,
      shippingFee = product.shippingFee,
      transactionFee = transactionFee,
    )
  }

  override fun calculateTax(buyerId: UUID, request: ShopPaymentCalculateTaxRequest): ShopPaymentCalculateTaxResponse {
    val buyer = shopUserRepository.findById(id = buyerId) ?: throw ShopError.USER_NOT_FOUND.asException()

    val shippingAddress = shopShippingAddressRepository.findDefaultByBuyerId(
      buyerId = buyer.id
    ) ?: throw ShopError.SHIPPING_ADDRESS_NOT_FOUND.asException()

    val shippingCountry = shopShippingCountryRepository.findById(
      id = shippingAddress.shippingCountryId
    ) ?: throw ShopError.SHIPPING_COUNTRY_NOT_FOUND.asException()

    val product = shopProductRepository.findById(
      id = request.productId
    ) ?: throw ShopError.PRODUCT_NOT_FOUND.asException()

    val amount = product.price * ShopConstant.ORDER_QUANTITY_DEFAULT

    val calculatedTaxFromStripe = runCatching {
      shopStripeManager.calculateTax(
        amount = amount,
        city = shippingAddress.city,
        state = shippingAddress.state,
        line = shippingAddress.address,
        postalCode = shippingAddress.zipCode,
        country = shippingCountry.alpha2Code
      ).taxAmountExclusive
    }.getOrDefault(0L)

    val taxAmount = BigDecimal(calculatedTaxFromStripe).divide(ShopConstant.ONE_DOLLAR_IN_CENTS)

    val productTax = shopProductTaxRepository.findBy(
      buyerId = buyer.id,
      productId = product.id,
      shippingAddressId = shippingAddress.id
    )

    productTax?.let {
      shopProductTaxRepository.update(id = it.id, taxAmount = taxAmount)
    } ?: shopProductTaxRepository.insert(
      entity = ShopProductTaxEntity(
        buyerId = buyer.id,
        taxAmount = taxAmount,
        productId = product.id,
        shippingAddressId = shippingAddress.id
      )
    ) ?: throw ShopError.PRODUCT_TAX_NOT_FOUND.asException()

    return ShopPaymentCalculateTaxResponse(
      buyerId = buyer.id,
      taxAmount = taxAmount,
      productId = product.id,
      shippingAddress = shippingAddress.id
    )
  }

  override fun createPayPalOrder(
    buyerId: UUID,
    request: ShopPaymentCreatePayPalOrderRequest,
  ): ShopPaymentCreatePayPalOrderResponse {
    val buyer = shopUserRepository.findById(id = buyerId) ?: throw ShopError.USER_NOT_FOUND.asException()

    val shippingAddress = shopShippingAddressRepository.findDefaultByBuyerId(
      buyerId = buyer.id
    ) ?: throw ShopError.SHIPPING_ADDRESS_NOT_FOUND.asException()

    val product = shopProductRepository.findById(
      id = request.productId
    ) ?: throw ShopError.PRODUCT_NOT_FOUND.asException()

    val productTax = shopProductTaxRepository.findBy(
      buyerId = buyer.id,
      productId = product.id,
      shippingAddressId = shippingAddress.id,
    ) ?: throw ShopError.PRODUCT_TAX_NOT_FOUND.asException()

    val total = (product.price * ShopConstant.ORDER_QUANTITY_DEFAULT) + productTax.taxAmount + product.shippingFee
    val transactionFee = calculateTransactionFee(amount = total, paymentMethod = PaymentMethod.PAYPAL)
    val payableAmount = total + transactionFee

    val payPalOrder = shopPayPalManager.createOrder(amount = payableAmount)

    return ShopPaymentCreatePayPalOrderResponse(
      orderId = payPalOrder.id(),
      status = payPalOrder.status(),
      createTime = payPalOrder.createTime(),
      expirationTime = payPalOrder.expirationTime(),
      totalAmount = payableAmount,
      taxAmount = productTax.taxAmount,
      shippingFee = product.shippingFee,
      transactionFee = transactionFee
    )
  }

  private fun purchase(
    orderQuantity: Int,
    buyer: ShopUserEntity,
    taxAmount: BigDecimal,
    shippingFee: BigDecimal,
    product: ShopProductView,
    orderStatus: ShopOrderStatus,
    paymentMethod: PaymentMethod,
    extPaymentId: String? = null,
    event: ShopEventEntity? = null,
    cancelledReason: String? = null,
    extPaymentStatus: String? = null,
    extPaymentMethodId: String? = null,
    shippingAddress: ShopShippingAddressEntity,
  ): Triple<ShopProductView, ShopOrderEntity, ShopTransactionEntity?> {
    return transaction(databaseContext.primary) {
      val total = (product.price * orderQuantity.toBigDecimal()) + taxAmount + shippingFee
      val earnedAmount = (product.price * orderQuantity.toBigDecimal()) + shippingFee
      val transactionFee = calculateTransactionFee(amount = total, paymentMethod = paymentMethod)
      val payableAmount = total + transactionFee

      val newPaymentMethod = shopPaymentMethodRepository.insert(
        entity = ShopPaymentMethodEntity(
          userId = buyer.id,
          paymentMethod = paymentMethod,
          extPaymentMethodId = extPaymentMethodId,
        )
      ) ?: throw ShopError.PAYMENT_METHOD_NOT_FOUND.asException()

      val newShippingDetail = shopShippingDetailRepository.insert(
        entity = ShopShippingDetailEntity(
          buyerId = shippingAddress.buyerId,
          fullName = shippingAddress.fullName,
          phone = shippingAddress.phone ?: buyer.phone,
          shippingCountryId = shippingAddress.shippingCountryId,
          state = shippingAddress.state,
          city = shippingAddress.city,
          zipCode = shippingAddress.zipCode,
          address = shippingAddress.address,
          aptUnitSuite = shippingAddress.aptUnitSuite
        )
      ) ?: throw ShopError.SHIPPING_DETAIL_NOT_FOUND.asException()

      val newOrder = shopOrderRepository.insert(
        entity = ShopOrderEntity(
          buyerId = buyer.id,
          status = orderStatus,
          taxAmount = taxAmount,
          currency = Currency.USD,
          shippingFee = shippingFee,
          totalAmount = payableAmount,
          sellerId = product.sellerId,
          cancelledReason = cancelledReason,
          paymentMethodId = newPaymentMethod.id,
          shippingDetailId = newShippingDetail.id,
          transactionFee = transactionFee
        )
      ) ?: throw ShopError.ORDER_NOT_FOUND.asException()

      val newOrderProduct = shopOrderProductRepository.insert(
        entity = ShopOrderProductEntity(
          eventId = event?.id,
          orderId = newOrder.id,
          productId = product.id,
          currency = Currency.USD,
          price = product.price,
          quantity = orderQuantity,
          totalAmount = product.price * orderQuantity.toBigDecimal()
        )
      ) ?: throw ShopError.ORDER_PRODUCT_NOT_FOUND.asException()

      if (orderStatus != ShopOrderStatus.SOLD) {
        return@transaction Triple(product, newOrder, null)
      }

      val newTransaction = shopTransactionRepository.insert(
        entity = ShopTransactionEntity(
          ownerId = buyer.id,
          orderId = newOrder.id,
          amount = earnedAmount,
          type = ShopTransactionType.PAYMENT,
          status = ShopTransactionStatus.SUCCESSFUL,
          description = "${buyer.username} purchased ${product.name} product within the payable amount is ${newOrder.currency.symbol}${newOrder.totalAmount}"
        )
      ) ?: throw ShopError.TRANSACTION_NOT_FOUND.asException()

      shopPaymentRepository.insert(
        entity = ShopPaymentEntity(
          method = paymentMethod,
          extPaymentId = extPaymentId,
          amount = newTransaction.amount,
          extPaymentStatus = extPaymentStatus,
          extPaymentMethod = extPaymentMethodId,
          transactionId = newTransaction.id,
          gateway = if (paymentMethod == PaymentMethod.PAYPAL) PaymentGateway.PAYPAL else PaymentGateway.STRIPE
        )
      ) ?: throw ShopError.PAYMENT_NOT_FOUND.asException()

      val newProduct = shopProductRepository.increaseSoldQuantity(
        id = product.id, quantity = newOrderProduct.quantity
      )?.let {
        // If the stock quantity equals the sold quantity, update the product with the sold-out flag
        if (it.stock == it.soldQuantity) shopProductRepository.update(id = it.id, isSoldOut = true) else it
      } ?: throw ShopError.PRODUCT_NOT_FOUND.asException()

      val seller = shopUserRepository.findById(id = newOrder.sellerId) ?: throw ShopError.USER_NOT_FOUND.asException()

      event?.let { e ->
        shopEventRepository.increaseTotalRevenues(id = e.id, revenueAmount = newTransaction.amount)
        val eventReport = shopEventReportRepository.findLatestEventReportBaseOnOrder(
          eventId = e.id,
          sellerId = seller.id
        )
        eventReport?.let {
          shopEventReportRepository.trackSoldOrderIds(
            id = it.id,
            orderIds = setOf(newOrder.id)
          )
          shopEventReportRepository.increaseTotalRevenues(id = it.id, amount = newTransaction.amount)
        }
      }

      val updatedTransaction = shopTransactionRepository.update(
        id = newTransaction.id,
        receiverId = seller.id,
        receiverOldBalance = seller.availableBalance,
        receiverNewBalance = seller.availableBalance.plus(newTransaction.amount),
      )

      shopUserRepository.updateAvailableBalance(
        id = seller.id,
        amount = earnedAmount
      ) ?: throw ShopError.USER_NOT_FOUND.asException()

      Triple(newProduct, newOrder, updatedTransaction)
    }
  }

  private fun calculateTransactionFee(amount: BigDecimal, paymentMethod: PaymentMethod): BigDecimal {
    return when (paymentMethod) {
      PaymentMethod.PAYPAL -> ShopConstant.FIXED_TRANSACTION_FEE_PAYPAL + (amount * ShopConstant.PERCENTAGE_TRANSACTION_FEE_PAYPAL).setScale(2, RoundingMode.HALF_UP)
      else -> ShopConstant.FIXED_TRANSACTION_FEE + (amount * ShopConstant.PERCENTAGE_TRANSACTION_FEE).setScale(2, RoundingMode.HALF_UP)
    }
  }
}