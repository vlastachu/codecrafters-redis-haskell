{-# LANGUAGE StrictData #-}

module Storage.Definition where

import qualified Control.Exception as E
import Data.Protocol.Types (RedisValue (SimpleString))
import qualified StmContainers.Map as SM
import qualified Storage.Entry as SE

data Storage = Storage
  { replicaOf :: String,
    storeMap :: SM.Map ByteString SE.StorageEntry
  }

data AppError
  = TypeMismatch String
  | NotFound String
  | RetryRequest
  deriving (Show, Generic, Exception)

safeAtomically :: STM () -> IO ()
safeAtomically = defaultAtomically ()

defaultAtomically :: a -> STM a -> IO a
defaultAtomically def action =
  atomically action
    `E.catch` \(e :: AppError) -> do
      putStrLn $ "[AppError] " <> show e
      pure def

-- | Создать новое хранилище
newStorage :: String -> IO Storage
newStorage replica = Storage replica <$> SM.newIO

getType :: Storage -> ByteString -> STM RedisValue
getType store key =
  SimpleString <$> do
    mVal <- SM.lookup key (storeMap store)
    pure $ case mVal of
      Just (SE.Array _) -> "array"
      Just (SE.String _) -> "string"
      Just (SE.Stream _) -> "stream"
      _ -> "none"
