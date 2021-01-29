// Databricks notebook source
import scala.util.control._

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "f84616aa-d6be-478e-b2fc-9ef7c9ca078b",
  "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope = "training-scope", key = "appsecrete"),
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/381a10df-8e85-43db-86e1-8893b075b027/oauth2/token")

val mounts = dbutils.fs.mounts()
val mountPath = "/mnt/data"
var isExist: Boolean = false
val outer = new Breaks;

outer.breakable {
  for(mount <- mounts) {
    if(mount.mountPoint == mountPath) {
      isExist = true;
      outer.break;
    }
  }
}

if(isExist) {
  println("Volume Mounting for Case Study Data Already Exist!")
}
else {
  dbutils.fs.mount(
    source = "abfss://casestudy@shivastorageaccount.dfs.core.windows.net/",
    mountPoint = "/mnt/data",
    extraConfigs = configs)
}

// COMMAND ----------

