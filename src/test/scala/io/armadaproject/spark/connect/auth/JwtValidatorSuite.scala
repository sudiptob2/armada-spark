/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.armadaproject.spark.connect.auth

import java.security.KeyPairGenerator
import java.security.interfaces.{RSAPrivateKey, RSAPublicKey}
import java.util.Date

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.exceptions.JWTVerificationException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JwtValidatorSuite extends AnyFunSuite with Matchers {

  private val keyPair = {
    val gen = KeyPairGenerator.getInstance("RSA")
    gen.initialize(2048)
    gen.generateKeyPair()
  }
  private val pub    = keyPair.getPublic.asInstanceOf[RSAPublicKey]
  private val priv   = keyPair.getPrivate.asInstanceOf[RSAPrivateKey]
  private val alg    = Algorithm.RSA256(pub, priv)
  private val issuer = "https://idp.test/"

  private def buildVerifier(audience: String = null) = {
    val b = JWT.require(alg).withIssuer(issuer)
    if (audience != null) b.withAudience(audience)
    b.build()
  }

  test("verifies a fresh token signed with the matching key") {
    val token = JWT
      .create()
      .withIssuer(issuer)
      .withSubject("alice@example.com")
      .withExpiresAt(new Date(System.currentTimeMillis() + 60000))
      .sign(alg)

    val validator = new JwtValidator(buildVerifier(), issuer, null)
    val jwt       = validator.verify(token)
    jwt.getSubject shouldBe "alice@example.com"
  }

  test("rejects a token with the wrong issuer") {
    val token = JWT
      .create()
      .withIssuer("https://attacker.test/")
      .withSubject("alice@example.com")
      .withExpiresAt(new Date(System.currentTimeMillis() + 60000))
      .sign(alg)

    val validator = new JwtValidator(buildVerifier(), issuer, null)
    a[JWTVerificationException] should be thrownBy validator.verify(token)
  }
}
