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
  { storeMap :: TVar (M.Map ByteString (ByteString, Maybe UTCTime)),
    timeCache :: TVar UTCTime
  }

-- | Создать новое хранилище и кэш времени
newStorage :: IO Storage
newStorage = do
  now <- getCurrentTime
  storeVar <- newTVarIO M.empty
  timeVar <- newTVarIO now

  -- фоновое обновление времени каждую секунду
  _ <- forkIO $ forever $ do
    threadDelay 100000
    now' <- getCurrentTime
    atomically $ writeTVar timeVar now'

  pure $ Storage storeVar timeVar

-- | Получить текущее время из кэша
getCachedTime :: Storage -> IO UTCTime
getCachedTime = readTVarIO . timeCache

-- | Установить ключ с значением и опциональным временем жизни (секунды)
setValue :: Storage -> ByteString -> ByteString -> Maybe Int -> IO ()
setValue store key val msec = do
  now <- getCachedTime store
  let stmMap = storeMap store
  let addTime s = addUTCTime (fromIntegral s) now
  let expireTime = addTime <$> msec
  atomically $ modifyTVar' stmMap (M.insert key (val, expireTime))

-- | Получить значение по ключу. Возвращает Nothing, если ключ не найден или истёк.
getValue :: Storage -> ByteString -> IO (Maybe ByteString)
getValue store key = do
  now <- getCachedTime store
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