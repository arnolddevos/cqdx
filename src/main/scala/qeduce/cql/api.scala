package qeduce
package cql

import anodyne.{HMaps, Rules}

object api
  extends Qeduce
  with CQLTypes
  with CQLActions
  with Constructions
  with HMaps
  with Rules
