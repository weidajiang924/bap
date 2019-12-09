package com.dmp_utils
import ch.hsr.geohash.GeoHash
object GeoHashUtil{
  def getGeoHashUtils(longtitude: Double, latitude: Double): String = {
    GeoHash.withCharacterPrecision(latitude, longtitude, 8).toBase32
  }
}
