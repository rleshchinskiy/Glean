{-
  Copyright (c) Meta Platforms, Inc. and affiliates.
  All rights reserved.

  This source code is licensed under the BSD-style license found in the
  LICENSE file in the root directory of this source tree.
-}

{-# LANGUAGE TypeApplications #-}

module Glean.Glass.Pretty.LSIF ( prettyLsifSignature ) where

import Data.Text ( Text )

import qualified Glean
import Glean.Angle as Angle
import Glean.Haxl.Repos as Glean ( RepoHaxl )
import Glean.Glass.Utils ( fetchData )
import Data.Text.Prettyprint.Doc
  (pretty,
   layoutSmart,
   LayoutOptions,
   SimpleDocStream,
   reAnnotateS)

import qualified Glean.Schema.Lsif.Types as LSIF
import Glean.Glass.Types ( SymbolId(..) )

prettyLsifSignature
  :: LayoutOptions
  -> LSIF.SomeEntity
  -> Glean.RepoHaxl u w (Maybe (SimpleDocStream (Maybe SymbolId)))
prettyLsifSignature opts (LSIF.SomeEntity_defn dm) = do
  LSIF.DefinitionMoniker_key{..} <- Glean.keyOf dm
  text <- fetchData (definitionHover (Glean.getId definitionMoniker_key_defn))
  let docStream = layoutSmart opts . pretty <$> text
  return $ reAnnotateS (const Nothing) <$> docStream
prettyLsifSignature _ _ = pure Nothing

definitionHover :: Glean.IdOf LSIF.Definition -> Angle Text
definitionHover defnId = var $ \hoverText ->
  hoverText `where_` [
    wild .= predicate @LSIF.DefinitionHover (
      rec $
        field @"defn" (asPredicate (factId defnId)) $
        field @"hover" (
          rec $
            field @"text" hoverText
          end)
      end)
  ]
