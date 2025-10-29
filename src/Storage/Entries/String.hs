module Storage.Entries.String where

import Control.Concurrent
import qualified StmContainers.Map as SM
import Storage.Definition
import qualified Storage.Entry as SE

setValue :: Storage -> ByteString -> ByteString -> Maybe Int -> IO ()
setValue store key val maybemsec = do
  let !entry = SE.String val
  safeAtomically $ SM.insert entry key (storeMap store)
  for_ maybemsec $ \msec -> do
    setDestroyTimer store key msec

setDestroyTimer :: Storage -> ByteString -> Int -> IO ()
setDestroyTimer store key msec = do
  _ <- forkIO $ do
    threadDelay (msec * 1000) -- переводим миллисекунды в микросекунды
    safeAtomically $ do
      SM.delete key (storeMap store)
  pure ()

getValue :: Storage -> ByteString -> IO (Maybe ByteString)
getValue store key =
  defaultAtomically Nothing $ do
    mVal <- SM.lookup key (storeMap store)
    case mVal of
      Just (SE.String val) -> pure $ Just val
      Nothing -> pure Nothing
      other -> throwSTM (TypeMismatch $ "Expected string, but got " <> show other)