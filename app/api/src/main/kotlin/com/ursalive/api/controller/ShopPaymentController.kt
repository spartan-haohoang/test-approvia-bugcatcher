package com.ursalive.api.controller

import com.ursalive.api.auth.ShopSecurityRule
import com.ursalive.api.auth.shopUserId
import com.ursalive.api.error.ShopError
import com.ursalive.cache.redis.lock.RedisLock
import com.ursalive.client.common.ShopPageableResponse
import com.ursalive.client.request.shop.ShopDetachPaymentMethodRequest
import com.ursalive.client.request.shop.ShopPaymentCalculateTaxRequest
import com.ursalive.client.request.shop.ShopPaymentCreateIntentRequest
import com.ursalive.client.request.shop.ShopPaymentCreatePayPalOrderRequest
import com.ursalive.client.request.shop.ShopPaymentPayRequest
import com.ursalive.client.response.shop.ShopDetachPaymentMethodResponse
import com.ursalive.client.response.shop.ShopMyPaymentMethodResponse
import com.ursalive.client.response.shop.ShopPaymentCalculateTaxResponse
import com.ursalive.client.response.shop.ShopPaymentCreateIntentResponse
import com.ursalive.client.response.shop.ShopPaymentCreatePayPalOrderResponse
import com.ursalive.client.response.shop.ShopPaymentPayResponse
import com.ursalive.client.response.shop.ShopPaymentSetupIntentResponse
import com.ursalive.logging.Logging
import com.ursalive.logging.info
import com.ursalive.manager.shop.ShopPaymentManager
import com.ursalive.repository.shop.ShopOrderRepository
import io.micronaut.http.HttpRequest
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.TaskExecutors
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.authentication.Authentication
import io.micronaut.validation.Validated
import io.swagger.v3.oas.annotations.tags.Tag
import javax.validation.Valid

@Validated
@Tag(name = "shop-payment")
@ExecuteOn(TaskExecutors.IO)
@Controller("/api/v1/shop/payment")
class ShopPaymentController(
  private val redisLock: RedisLock,
  private val shopPaymentManager: ShopPaymentManager,
  private val shopOrderRepository: ShopOrderRepository
) : Logging {

  @Secured(
    ShopSecurityRule.BUYER,
    ShopSecurityRule.SELLER
  )
  @Post("/setup-intent")
  fun setupIntent(
    authentication: Authentication,
  ): ShopPaymentSetupIntentResponse {
    return shopPaymentManager.setupIntent(userId = authentication.shopUserId)
  }

  @Secured(
    ShopSecurityRule.BUYER,
    ShopSecurityRule.SELLER
  )
  @Post("/create-intent")
  fun createIntent(
    authentication: Authentication,
    @Valid @Body request: ShopPaymentCreateIntentRequest,
  ): ShopPaymentCreateIntentResponse {
    return shopPaymentManager.createIntent(buyerId = authentication.shopUserId, request = request)
  }

  @Secured(
    ShopSecurityRule.BUYER,
    ShopSecurityRule.SELLER
  )
  @Post("/create-paypal-order")
  fun createPayPalOrder(
    authentication: Authentication,
    @Valid @Body request: ShopPaymentCreatePayPalOrderRequest,
  ): ShopPaymentCreatePayPalOrderResponse {
    return shopPaymentManager.createPayPalOrder(buyerId = authentication.shopUserId, request = request)
  }

  @Secured(
    ShopSecurityRule.BUYER,
    ShopSecurityRule.SELLER
  )
  @Post("/detach-payment-method")
  fun detachPaymentMethod(
    @Valid @Body request: ShopDetachPaymentMethodRequest,
  ): ShopDetachPaymentMethodResponse {
    return shopPaymentManager.detachPaymentMethod(request = request)
  }

  @Secured(
    ShopSecurityRule.BUYER,
    ShopSecurityRule.SELLER
  )
  @Get("/payment-methods")
  fun retrievePaymentMethods(
    authentication: Authentication,
  ): ShopPageableResponse<ShopMyPaymentMethodResponse> {
    return shopPaymentManager.retrievePaymentMethods(userId = authentication.shopUserId)
  }

  @Secured(
    ShopSecurityRule.BUYER,
    ShopSecurityRule.SELLER
  )
  @Post("/pay")
  fun pay(
    httpRequest: HttpRequest<*>,
    authentication: Authentication,
    @Valid @Body request: ShopPaymentPayRequest,
  ): ShopPaymentPayResponse {
    val lock = redisLock.create("shop-payment-pay-${httpRequest.remoteAddress}")
    return if (lock.tryLock()) {
      try {
        info("Payment Product Lock created")
        val userId = authentication.shopUserId
        val response = shopPaymentManager.pay(userId, request)
        if (shopOrderRepository.findById(response.orderId) != null) {
          throw ShopError.ORDER_PRODUCT_NOT_FOUND.asException()
        }
        return response
      } finally {
        info("Payment Product Lock released")
        lock.unlock()
      }
    } else {
      throw ShopError.PAYMENT_PAY_LOCK_ACQUIRED_BY_ANOTHER.asException()
    }
  }

  @Secured(
    ShopSecurityRule.BUYER,
    ShopSecurityRule.SELLER
  )
  @Post("/calculate-tax")
  fun calculateTax(
    authentication: Authentication,
    @Valid @Body request: ShopPaymentCalculateTaxRequest,
  ): ShopPaymentCalculateTaxResponse {
    return shopPaymentManager.calculateTax(buyerId = authentication.shopUserId, request = request)
  }
}