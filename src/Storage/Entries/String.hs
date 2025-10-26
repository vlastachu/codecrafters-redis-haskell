module Storage.Entries.String where

import Data.Time.Clock
import qualified StmContainers.Map as SM
import Storage.Definition
import qualified Storage.Entry as SE

-- | Установить ключ с значением и опциональным временем жизни (миллисекунды)
setValue :: Storage -> ByteString -> ByteString -> Maybe Int -> IO ()
setValue store key val maybemsec = do
  for_ maybemsec (setDestroyTimer store key)
  let !entry = SE.String val
  safeAtomically $ SM.insert entry key (storeMap store)

setDestroyTimer :: Storage -> ByteString -> Int -> IO ()
setDestroyTimer store key msec = do
  now <- getCurrentTime
  let !expireTime = addUTCTime (fromRational (toRational msec / 1000)) now
  safeAtomically $ SM.insert expireTime key (destroyTimerMap store)

-- | Получить значение по ключу (Nothing, если не найдено или истекло)
getValue :: Storage -> ByteString -> IO (Maybe ByteString)
getValue store key = do
  now <- getCurrentTime
  defaultAtomically Nothing $ do
    mVal <- SM.lookup key (storeMap store)
    mDestroyTimer <- SM.lookup key (destroyTimerMap store)
    case (mVal, mDestroyTimer) of
      (Nothing, _) -> pure Nothing
      (Just (SE.String val), Nothing) -> pure (Just val)
      (Just (SE.String val), Just expTime) ->
        if now < expTime
          then val `seq` pure (Just val)
          else do
            SM.delete key (storeMap store)
            pure Nothing
      value -> throwSTM (TypeMismatch $ "Expected string, but got " <> show value)
