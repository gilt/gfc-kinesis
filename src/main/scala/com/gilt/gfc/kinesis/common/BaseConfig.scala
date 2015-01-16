package com.gilt.gfc.kinesis.common

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
}
