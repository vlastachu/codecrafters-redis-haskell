module Network.Network
  ( runServer,
  )
where

import Control.Concurrent (forkFinally)
import Data.Attoparsec.ByteString (IResult (..), parseWith)
import qualified Data.ByteString.Char8 as BS
import Data.Protocol.Decode
import Data.Protocol.Encode
import Data.Protocol.Types
import Data.Request
import qualified Data.Text.IO as TIO
import Logic.CommandHandler
import Network.Socket
import Network.Socket.ByteString (recv, sendAll)
import Storage.Storage
import Network.ClientState

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
    -- мешает бенчмаркам
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

handleClient :: Socket -> Storage -> IO ()
handleClient sock store = do
  clientState <- newIORef initState
  loop BS.empty clientState
  where
    loop :: BS.ByteString -> IORef ClientState -> IO ()
    loop buffer clientState = do
      result <- parseWith (recv sock 512) parseRedisValue buffer
      case result of
        Fail _ _ err
          | err == "not enough input" -> pure () -- EOF
          | otherwise -> do
              sendAll sock (encode $ ErrorString $ "parse error: " <> show err)
              TIO.hPutStrLn stderr $ "parse failed; error: " <> show err
        Done rest value -> do
          resp <- executeCommand store clientState (decodeRequest value)
          sendAll sock resp
          loop rest clientState
        _ -> TIO.hPutStrLn stderr "unexpected state"

executeCommand :: Storage -> IORef ClientState -> Either Text Request -> IO BS.ByteString
executeCommand _ _ (Left err) = pure $ encode $ ErrorString $ show err
executeCommand store clientState (Right req) = do
  response <- handleTx store clientState req
  pure $ encode response
