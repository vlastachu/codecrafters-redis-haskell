{-# LANGUAGE StrictData #-}

module Storage.Definition where

import qualified Control.Exception as E
import qualified StmContainers.Map as SM
import qualified Storage.Entry as SE

newtype Storage = Storage
  { storeMap :: SM.Map ByteString SE.StorageEntry
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
newStorage :: IO Storage
newStorage = Storage <$> SM.newIO

getType :: Storage -> ByteString -> IO ByteString
getType store key = defaultAtomically "none" $ do
  mVal <- SM.lookup key (storeMap store)
  pure $ case mVal of
    Just (SE.Array _) -> "array"
    Just (SE.String _) -> "string"
    Just (SE.Stream _) -> "stream"
    _ -> "none"
