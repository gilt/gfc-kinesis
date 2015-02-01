package com.gilt.gfc.kinesis.common

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}

trait BaseConfig {
  /**
   * Specify the Amazon region name to be used.
   *
   * Defaults to "us-east-1"
   * @return
   */
  def regionName: String = "us-east-1"

  /**
   * Allow specific identification of the kinesis endpoint.
   *
   * @return defaults to None
   */
  def kinesisEndpoint: Option[String] = None

  /**
   * Get the AWS CredentialsProvider for Kinesis, etc., access.
   *
   * Defaults to [[com.amazonaws.auth.DefaultAWSCredentialsProviderChain]]
   *
   * @return
   */
  def awsCredentialsProvider: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain()

  /**
   * Allow for customised low level configuration of the Amazon AWS Client.
   *
   * Note, specifying a value here may override specified other configurations.
   *
   * @return Default is to use the default AWS configuration - this is likely a reasonable value in most cases.
   */
  def awsClientConfig: Option[ClientConfiguration] = None
}
