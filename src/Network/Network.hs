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
import qualified Data.Text.IO as TIO
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
  TIO.hPutStrLn stderr $ "Server listening on port " <> show defaultRedisPort
  forever $ do
    (conn, _) <- accept sock
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
      TIO.hPutStrLn stderr $ "Connection" <> show conn <> " closed by exceptio: " <> show someExc

-- | Обработка одного клиента
handleClient :: Socket -> Storage -> IO ()
handleClient sock store = forever $ do
  msg <- recv sock 4096
  msg `seq` pure ()
  if BS.null msg
    then pure () -- hPutStrLn stderr  "Connection closed"
    else do
      case decodeRequest =<< decode msg of
        Left err -> do
          sendAll sock (encode $ ErrorString $ "decode error: " <> show err)
          TIO.hPutStrLn stderr $ "parse failed: " <> show msg <> "; error: " <> err
        Right req -> do
          resp <- executeCommand store req
          resp `seq` sendAll sock resp

-- | Выполнить команду и вернуть RESP-ответ
executeCommand :: Storage -> Request -> IO BS.ByteString
executeCommand store req = do
  response <- handleCommand store req
  pure $ encode $ encodeResponse response
