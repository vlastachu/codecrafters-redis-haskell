{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

{-# HLINT ignore "Use concatMap" #-}
module Data.Protocol.Encode (encode) where

import qualified Data.ByteString as BS hiding (map)
import Data.Protocol.Types
import Prelude hiding (concat, concatMap)

_rn :: ByteString
_rn = "\r\n"

encode :: RedisValue -> ByteString
encode (SimpleString s) = "+" <> s <> _rn
encode (ErrorString s) = "-" <> s <> _rn
encode NilString = "$-1\r\n"
encode (BulkString s) = BS.concat ["$", show $ BS.length s, _rn, s, _rn]
encode (Integer n) = ":" <> show n <> _rn
encode NilArray = "*-1\r\n"
encode (Array xs) =
  BS.concat ["*", show $ length xs, _rn, BS.concat (map encode xs)]
