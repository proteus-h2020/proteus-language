package eu.stratosphere.emma
package compiler

import eu.stratosphere.emma.compiler.lang.{Core, Source}

import scala.reflect.api.Universe

/**
 * Base compiler trait.
 *
 * This trait has to be instantiated with an underlying universe and works for both runtime and
 * compile time reflection.
 */
trait Compiler extends Source with Core {

  /** The underlying universe object. */
  override val universe: Universe
}
