module Storage.Entries.Stream where

import Control.Concurrent
import qualified Control.Monad.STM as STM
import Data.Sequence (ViewL (..), (><))
import qualified Data.Sequence as Seq
import Data.Time.Clock (NominalDiffTime)
import GHC.Clock (getMonotonicTimeNSec)
import qualified StmContainers.Map as SM
import Storage.Definition
import qualified Storage.Entry as SE

-- | Добавить запись в поток.
-- Принимает готовый timestamp (в миллисекундах) и список полей.
-- seqNum всегда 0
addStreamEntry :: Word64 -> [(ByteString, ByteString)] -> SE.StreamData -> SE.StreamData
addStreamEntry timestamp fields' sdata =
  SE.StreamData
    { SE.entries = SE.StreamEntry newID fields' : SE.entries sdata,
      SE.lastID = newID
    }
  where
    newID = SE.StreamID timestamp 0

getTimestampNs :: IO Word64
getTimestampNs = getMonotonicTimeNSec

setStream :: Storage -> ByteString -> SE.StreamData -> STM ()
setStream store key val = SM.insert (SE.Stream val) key (storeMap store)

xadd :: Storage -> ByteString -> [(ByteString, ByteString)] -> IO ByteString
xadd store key entries = do
  timestamp <- getTimestampNs
  defaultAtomically "" $ do
    mStream <- SM.lookup key (storeMap store)
    newStream <- case mStream of
      Just (SE.Stream stream) -> pure $ addStreamEntry timestamp entries stream
      Nothing ->
        pure $
          SE.StreamData
            { SE.entries = [SE.StreamEntry (SE.StreamID timestamp 0) entries],
              SE.lastID = SE.StreamID timestamp 0
            }
      value -> throwSTM (TypeMismatch $ "Expected array, but got: " <> show value)
    setStream store key newStream
    pure $ show $ SE.lastID newStream
