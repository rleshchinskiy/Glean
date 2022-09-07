{-
  Copyright (c) Meta Platforms, Inc. and affiliates.
  All rights reserved.

  This source code is licensed under the BSD-style license found in the
  LICENSE file in the root directory of this source tree.
-}

module Glean.Bytecode.Generate.Haskell (main)
where

import Data.Bits (shiftL)
import Data.Char (toLower)
import Data.List (intercalate, intersperse, stripPrefix)
import Data.Maybe
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Text.IO as Text
import Data.Word (Word64)
import Numeric (showHex)
import System.Directory
import System.Environment
import System.Exit (die)
import System.FilePath

import Glean.Bytecode.Generate.Instruction
import Glean.Bytecode.Types

-- We generate two modules:
--
-- Instruction: contains the Insn type and some utility functions
--
-- Issue: a typed interface for generating instructions based on the MonadInsn
-- class from Glean.RTS.Bytecode.MonadInsn.

list :: Text -> Text -> [Text] -> [Text]
list first others (x:xs) = first <> x : map (others <>) xs
list _ _ [] = []

modPfx :: Text
modPfx = "Glean.RTS.Bytecode.Gen"

modPath :: FilePath
modPath = Text.unpack $ Text.replace "." "/" modPfx

main :: IO ()
main = do
  args <- getArgs
  dir <- case args of
    [arg] | Just dir <- stripPrefix "--install_dir=" arg ->
      return $ dir </> modPath
    _ -> die "invalid arguments"
  createDirectoryIfMissing True dir
  genModule dir "Instruction"
    []
    [ "Insn(..)"
    , "mapLabels"
    , "insnLabels"
    , "insnSize"
    , "insnWords"
    , "insnShow" ] $
    intercalate [""]
    [ [ "import Control.Monad (replicateM)"
      , "import Data.Bits"
      , "import Data.Int (Int8, Int32)"
      , "import Data.Word (Word8, Word32, Word64)"
      , "import Text.Show (showListWith)"
      , "import qualified Glean.Bytecode.Decode as D"
      , "import Glean.Bytecode.Types" ]
    , genInsnType
    , genMapLabels
    , genInsnLabels
    , genInsnSize
    , genInsnWords
    , genDecodable
    , genInsnShow ]

  genModule dir "Issue" [] (map (varName . insnName) instructions) $
    [ "import Data.ByteString (ByteString)"
    , "import Data.Int (Int8, Int32)"
    , "import Data.Word (Word8, Word32, Word64)"
    , "import Glean.RTS.Bytecode.Gen.Instruction (Insn(..))"
    , "import Glean.RTS.Bytecode.Code"
    , "import Glean.Bytecode.Types"
    , "" ]
    ++ genIssue

  genModule dir "Version" [] ["version", "lowestSupportedVersion"]
    [ "version :: Int"
    , "version = " <> Text.pack (show version)
    , ""
    , "lowestSupportedVersion :: Int"
    , "lowestSupportedVersion = " <> Text.pack (show lowestSupportedVersion)
    ]

genModule :: FilePath -> String -> [Text] -> [Text] -> [Text] -> IO ()
genModule path name exts es ls =
  Text.writeFile (path </> name <.> "hs")
  $ Text.unlines $
  [ "-- @" <> "generated" ]
  ++
  [ "{-# LANGUAGE " <> Text.intercalate ", " exts <> " #-}" | not (null exts)]
  ++
  [ "{-# OPTIONS_GHC -Wno-unused-matches #-}"
  , "module " <> modPfx <> "." <> Text.pack name ]
  ++ list "  ( " "  , " es ++
  [ "  ) where"
  , "" ]
  ++ ls

-- | Generate the Insn type. We parametrise over the types of registers and
-- labels.
genInsnType :: [Text]
genInsnType = "data Insn where" : map genInsn instructions
  where
    genInsn Insn{..} =
      "  "
        <> insnName
        <> " :: "
        <> Text.concat [genArgTy (argTy arg) <> " -> " | arg <- insnArgs]
        <> "Insn"

    genArgTy (Imm ty) = "{-# UNPACK #-} !" <> immTy ty
    genArgTy (Reg var ty _) = "{-# UNPACK #-} !(" <> showRegTy var ty <> ")"
    genArgTy Offsets = "[Label]"
    genArgTy Regs = "[Register 'Word]"

    immTy :: ImmTy -> Text
    immTy U8 = "Word8"
    immTy I8 = "Int8"
    immTy U32 = "Word32"
    immTy I32 = "Int32"
    immTy ImmOffset = "Label"
    immTy ImmLit = "Literal"

showTy :: Ty -> Text
showTy ty = "'" <> Text.pack (show ty)

showRegTy :: Maybe Text -> Ty -> Text
showRegTy var ty = "Register " <> fromMaybe (showTy ty) var

genMapLabels :: [Text]
genMapLabels =
  "mapLabels :: (Label -> Label) -> Insn -> Insn" :
  [ "mapLabels f ("
      <> Text.unwords (insnName insn : map argName (insnArgs insn))
      <> ") = "
      <> Text.unwords (insnName insn : exprs)
    | insn <- instructions
    , let exprs = map mkMap (insnArgs insn)
    , any (\(expr,arg) -> expr /= argName arg) $ zip exprs $ insnArgs insn ]
  ++
  [ "mapLabels _ insn = insn"]
  where
    mkMap (Arg name (Imm ImmOffset)) = "(f " <> name <> ")"
    mkMap (Arg name Offsets) = "(map f " <> name <> ")"
    mkMap (Arg name _) = name

genInsnLabels :: [Text]
genInsnLabels =
  "insnLabels :: Insn -> [Label]" :
  [ "insnLabels ("
      <> Text.unwords (insnName insn : map argName (insnArgs insn))
      <> ") = "
      <> Text.intercalate " ++ " exprs
    | insn <- instructions
    , let exprs = mapMaybe mkList (insnArgs insn)
    , not $ null exprs ]
  ++
  [ "insnLabels _ = []"]
  where
    mkList (Arg name (Imm ImmOffset)) = Just $ "[" <> name <> "]"
    mkList (Arg name Offsets) = Just name
    mkList Arg{} = Nothing

-- | Generates a function that yields the size of an instruction in words.
genInsnSize :: [Text]
genInsnSize =
  "insnSize :: Insn -> Word64" :
  [ "insnSize ("
      <> Text.unwords (insnName : map mkArg insnArgs)
      <> ") = 1"
      <> dyn
    | Insn{..} <- instructions
    , let dyn = Text.concat (map dynSize insnArgs)
    , not $ Text.null dyn ]
  ++
  [ "insnSize _ = 1" ]
  where
    mkArg (Arg name Offsets) = name
    mkArg (Arg name Regs) = name
    mkArg _ = "_"

    dynSize (Arg name Offsets) = " + fromIntegral (length " <> name <> ")"
    dynSize (Arg name Regs) = " + fromIntegral (length " <> name <> ")"
    dynSize _ = ""

-- | Generates a function that encodes an instruction as a list of words.
genInsnWords :: [Text]
genInsnWords =
   [ "insnWords"
   , "  :: (forall ty. Register ty -> Word64) -> (Label -> Word64) -> Insn -> [Word64]" ]
   ++
   [ "insnWords fromReg fromLabel ("
      <> insnPattern id insn
      <> ") = ["
      <> Text.pack (show op)
      <> Text.concat [" .|. " <> encode opd | opd <- insnOperands  insn]
      <> "]"
      <> Text.concat (map argWords $ insnArgs insn)
        | (op, insn) <- zip [0 :: Int ..] instructions ]
    where
      encode Opd{..} =
          "(" <> argValue opdArg
            <> " `unsafeShiftL` " <> Text.pack (show opdStart) <> ")"

      argValue (Arg name (Imm ty)) = immValue name ty
      argValue (Arg name Reg{}) = "fromReg " <> name
      argValue (Arg name Offsets) = "fromIntegral (length " <> name <> ")"
      argValue (Arg name Regs) = "fromIntegral (length " <> name <> ")"

      immValue :: Text -> ImmTy -> Text
      immValue name U8 = "fromIntegral " <> name
      immValue name I8 = "fromIntegral " <> name
      immValue name U32 = "fromIntegral " <> name
      immValue name I32 = "fromIntegral " <> name
      immValue name ImmOffset = "(fromLabel " <> name <> " .&. 0xFFFFFFFF)"
      immValue name ImmLit = "fromLiteral " <> name

      argWords (Arg name Offsets) = "++ map fromLabel " <> name
      argWords (Arg name Regs) = "++ map fromReg " <> name
      argWords _ = ""

insnPattern :: (Text -> Text) -> Insn -> Text
insnPattern cname Insn{..} =
  Text.unwords $ cname insnName : map argName insnArgs

varName :: Text -> Text
varName name
  | Just (c,rest) <- Text.uncons name = Text.cons (toLower c) rest
  | otherwise = name

genIssue :: [Text]
genIssue = intercalate [""]
  [ [ varName insnName
        <> " :: "
        <> context insnContext
        <> Text.unwords
            (intersperse "->"
              $ map (genArgType . argTy) insnArgs ++ ["Code ()"])
    , insnPattern varName insn <> " = do"
    ] ++ map ("  " <>)
      (mapMaybe literal insnArgs ++
      [ issue insnEffects <> " $ " <> Text.unwords (insnName : map genArgRef insnArgs) ])
    | insn@Insn{..} <- instructions ]
  where
    context [] = ""
    context [c] = c <> " => "
    context cs = "(" <> Text.intercalate ", " cs <> ") => "

    genArgType (Imm ty) = immArgType ty
    genArgType (Reg var ty _) = showRegTy var ty
    genArgType Offsets = "[Label]"
    genArgType Regs = "[Register 'Word]"

    genArgRef (Arg name (Imm ImmLit)) = name <> "_i"
    genArgRef (Arg name _) = name

    literal (Arg name (Imm ImmLit)) = Just $
      name <> "_i <- literal " <> name
    literal _ = Nothing

    issue effects
      | EndBlock `elem` effects = "issueEndBlock"
      | otherwise = "issue"

    immArgType :: ImmTy -> Text
    immArgType U8 = "Word8"
    immArgType I8 = "Int8"
    immArgType U32 = "Word32"
    immArgType I32 = "Int32"
    immArgType ImmOffset = "Label"
    immArgType ImmLit = "ByteString"

genDecodable :: [Text]
genDecodable =
  [ "instance D.Decodable Insn where"
  , "  decode = do"
  , "    insn <- D.decode"
  , "    let op = fromIntegral (insn :: Word64) :: Word8"
  , "    case op of" ]
  ++
  concat [ [ "      " <> Text.pack (show i) <> " -> do" ]
    ++ map ("        " <> ) (concatMap decodeOpd $ insnOperands insn)
    ++
    [ "        return $ " <> Text.unwords (insnName : map argName insnArgs) ]
    | (i, insn@Insn{..}) <- zip [0 :: Int ..] instructions ]
  ++
  [ "      _ -> fail $ \"invalid opcode \" ++ show op"]
  where
    decodeOpd Opd{..} = decodeArg opdArg $
       "(insn `unsafeShiftR` " <> Text.pack (show opdStart) <> ") .&. 0x"
        <> Text.pack (showHex (1 `shiftL` opdSize - 1 :: Word64) "")

    decodeArg (Arg name (Imm ty)) val =
      [ "let " <> name <> " = " <> decodeImm ty <> " $ " <> val ]
    decodeArg (Arg name Reg{}) val =
      [ "let " <> name <> " = Register $ " <> val ]
    decodeArg (Arg name Offsets) val =
      [ name <> " <- replicateM (fromIntegral $ " <> val <> ") $ " <> label <> " <$> D.decode"]
    decodeArg (Arg name Regs) val =
      [ name <> " <- replicateM (fromIntegral $ " <> val <> ") D.decode"]

    decodeImm U8 = "fromIntegral"
    decodeImm I8 = "fromIntegral"
    decodeImm U32 = "fromIntegral"
    decodeImm I32 = "fromIntegral"
    decodeImm ImmOffset = label
    decodeImm ImmLit = "Literal"

    label = "(Label . fromIntegral . (fromIntegral :: Word64 -> Int32))"

genInsnShow :: [Text]
genInsnShow =
  [ "insnShow"
  , "  :: (Label -> String)"
  , "  -> (forall t. Register t -> String)"
  , "  -> Insn"
  , "  -> String" ]
  ++
  [ "insnShow showLabel showReg ("
      <> insnPattern id insn
      <> ") = concat [\"" <> insnName <> "\""
      <> Text.intercalate ", \",\""
          [", \' \' : " <> showArg arg | arg <- insnArgs]
      <> "]" | insn@Insn{..} <- instructions ]
    where
      showArg (Arg name (Imm ImmOffset)) = "showLabel " <> name
      showArg (Arg name (Imm ImmLit)) = "show (fromLiteral " <> name <> ")"
      showArg (Arg name Imm{}) = "show " <> name
      showArg (Arg name Reg{}) = "showReg " <> name
      showArg (Arg name Offsets) =
        "showListWith (showString . showLabel) " <> name <> " \"\""
      showArg (Arg name Regs) =
        "showListWith (showString . showReg) " <> name <> " \"\""
