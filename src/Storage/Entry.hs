{-# LANGUAGE InstanceSigs #-}

module Storage.Entry where

import qualified Text.Show (Show (show))

data StorageEntry
  = String ByteString
  | Array (Seq ByteString)
  | Stream [StreamEntry]
  deriving (Eq, Show)

data StreamEntry = StreamEntry
  { entryID :: StreamID,
    fields :: [(ByteString, ByteString)]
  }
  deriving (Eq, Show)

data StreamID = StreamID
  { timestamp :: Word64,
    seqNum :: Word64
  }
  deriving (Eq, Ord)

instance Show StreamID where
  show :: StreamID -> String
  show (StreamID ts seqn) = show ts ++ "-" ++ show seqn
