module Network.Network
  ( runServer,
  )
where

import Control.Concurrent (forkFinally)
import Data.Attoparsec.ByteString (IResult (..), parse)
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
      -- мешает бенчмаркам
      -- TIO.hPutStrLn stderr $ "Connection" <> show conn <> " closed by exceptio: " <> show someExc
      pure ()

-- | Обработка одного клиента
handleClient :: Socket -> Storage -> IO ()
handleClient sock store = go BS.empty
  where
    go buffer = do
      chunk <- recv sock 512
      if BS.null chunk
        then pure () -- соединение закрыто
        else do
          let input = buffer <> chunk
          case parse parseRedisValue input of
            Done rest value -> do
              resp <- executeCommand store (decodeRequest value)
              sendAll sock resp
              go rest
            Partial _cont -> do
              -- не хватило данных — ждём следующий кусок, просто оставляем buffer
              go input
            Fail _ _ err -> do
              sendAll sock (encode $ ErrorString $ "parse error: " <> show err)
              TIO.hPutStrLn stderr $ "parse failed: " <> show input <> "; error: " <> show err

executeCommand :: Storage -> Either Text Request -> IO BS.ByteString
executeCommand _ (Left err) = pure $ encode $ ErrorString $ show err
executeCommand store (Right req) = do
  response <- handleCommand store req
  pure $ encode $ encodeResponse response
