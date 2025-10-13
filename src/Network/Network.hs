module Network.Network
  ( runServer,
  )
where

import Control.Concurrent (forkFinally)
import qualified Data.ByteString.Char8 as BS
import Data.Protocol.Decode
import Data.Protocol.Encode
import Data.Protocol.Types
import Data.Request
import Data.Response
import Logic.CommandHandler
import Network.Socket
import Network.Socket.ByteString (recv, sendAll)
import Storage.Storage

defaultRedisPort :: ServiceName
defaultRedisPort = "6379"

-- | Запуск TCP-сервера
runServer :: Storage -> IO ()
runServer store = do
  addr <- resolve
  sock <- open addr
  hPutStrLn stderr StrLn $ "Server listening on port " ++ defaultRedisPort
  forever $ do
    (conn, peer) <- accept sock
    -- hPutStrLn stderr  $ "Connection from " ++ show peer
    void $ forkFinally (handleClient conn store) (close'' conn)
  where
    resolve = do
      let hints = defaultHints {addrFlags = [AI_PASSIVE], addrSocketType = Stream}
      addr : _ <- getAddrInfo (Just hints) Nothing (Just defaultRedisPort)
      pure addr

    open addr = do
      sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
      setSocketOption sock ReuseAddr 1
      bind sock (addrAddress addr)
      listen sock 10
      pure sock

    close'' conn someExc = do
      close conn
      print someExc
      hPutStrLn stderr  $ "Connection closed" <> show conn

-- | Обработка одного клиента
handleClient :: Socket -> Storage -> IO ()
handleClient sock store = forever $ do
  msg <- recv sock 4096
  if BS.null msg
    then pure () -- hPutStrLn stderr  "Connection closed"
    else do
      case decodeRequest <$> decode msg of
        Left err -> do
          sendAll sock (encode $ ErrorString $ "decode error: " <> show err)
          hPutStrLn stderr  $ "parse failed: " <> show msg <> "; error: " <> err
        Right Nothing -> do
          sendAll sock (encode $ ErrorString "decode error;")
          hPutStrLn stderr  $ "parse failed: " <> show msg <> "; error: Nope :("
        Right (Just req) -> do
          resp <- executeCommand store req
          sendAll sock resp

-- | Выполнить команду и вернуть RESP-ответ
executeCommand :: Storage -> Request -> IO BS.ByteString
executeCommand store req = do
  response <- handleCommand store req
  pure $ encode $ encodeResponse response
