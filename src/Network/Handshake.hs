module Network.Handshake where

import Data.AppArgs
import Data.Protocol.Encode
import Data.Request (Request (..))
import Data.Request.Encode
import Network.Socket
import Network.Socket.ByteString (sendAll)

splitHostPort :: Text -> (String, String)
splitHostPort s =
  case words s of
    [h, p] -> (strip $ show h, strip $ show p)
    _ -> error $ "Invalid address: " <> s
  where
    strip = filter (/= '"')

checkHandshake :: AppArgs -> IO ()
checkHandshake (AppArgs _ "") = pure ()
checkHandshake (AppArgs ownPort address) = do
  let (host, port) = splitHostPort address

  let hints =
        defaultHints
          { addrSocketType = Stream
          }

  addr : _ <- getAddrInfo (Just hints) (Just host) (Just port)
  sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
  connect sock (addrAddress addr)
  let execute = sendAll sock . encode . encodeRequest
  execute Ping
  execute $ ReplConf "listening-port" (show ownPort)
  execute $ ReplConf "capa" "psync2"
