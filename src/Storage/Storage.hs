module Storage.Storage
  ( Storage,
    newStorage,
    setValue,
    getValue,
  )
where

import Control.Concurrent
import qualified Data.Map.Strict as M
import Data.Time.Clock

data Storage = Storage
  { storeMap :: TVar (M.Map ByteString (ByteString, Maybe UTCTime))
  }

-- | Создать новое хранилище и кэш времени
newStorage :: IO Storage
newStorage = do
  now <- getCurrentTime
  storeVar <- newTVarIO M.empty

  pure $ Storage storeVar

-- | Установить ключ с значением и опциональным временем жизни (секунды)
setValue :: Storage -> ByteString -> ByteString -> Maybe Int -> IO ()
setValue store key val msec = do
  now <- getCurrentTime
  let stmMap = storeMap store
  let addMillseconds ms = addUTCTime (fromRational (toRational ms / 1000)) now
  let expireTime = addMillseconds <$> msec
  atomically $ modifyTVar' stmMap (M.insert key (val, expireTime))

-- | Получить значение по ключу. Возвращает Nothing, если ключ не найден или истёк.
getValue :: Storage -> ByteString -> IO (Maybe ByteString)
getValue store key = do
  now <- getCurrentTime
  let stmMap = storeMap store
  atomically $ do
    m <- readTVar stmMap
    case M.lookup key m of
      Nothing -> pure Nothing
      Just (val, Nothing) -> pure (Just val)
      Just (val, Just expTime) ->
        if now < expTime
          then pure (Just val)
          else do
            modifyTVar' stmMap (M.delete key)
            pure Nothing