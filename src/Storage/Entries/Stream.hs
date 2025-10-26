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
addStreamEntry :: ByteString -> [(ByteString, ByteString)] -> [SE.StreamEntry] -> [SE.StreamEntry]
addStreamEntry timestamp fields' sdata =
  SE.StreamEntry timestamp fields' : sdata

getTimestampNs :: IO Word64
getTimestampNs = getMonotonicTimeNSec

lastID :: [SE.StreamEntry] -> STM ByteString
lastID (last : _) = pure $ SE.entryID last
lastID _ = throwSTM $ NotFound "empty stream"

setStream :: Storage -> ByteString -> [SE.StreamEntry] -> STM ()
setStream store key val = SM.insert (SE.Stream val) key (storeMap store)

xadd :: Storage -> ByteString -> ByteString -> [(ByteString, ByteString)] -> IO ByteString
xadd store key entryKey entries = defaultAtomically "" $ do
  mStream <- SM.lookup key (storeMap store)
  newStream <- case mStream of
    Just (SE.Stream stream) -> pure $ addStreamEntry entryKey entries stream
    Nothing -> pure [SE.StreamEntry entryKey entries]
    value -> throwSTM (TypeMismatch $ "Expected array, but got: " <> show value)
  setStream store key newStream
  lastID newStream
