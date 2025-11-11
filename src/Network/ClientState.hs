module Network.ClientState where

import Data.Request

data ClientState = ClientState
  { isTxReceiving :: Bool,
    txs :: [Request]
  }

initState :: ClientState
initState = ClientState {isTxReceiving = False, txs = []}

startTx :: IORef ClientState -> IO ()
startTx ref = writeIORef ref $ ClientState {isTxReceiving = True, txs = []}

finishTx :: IORef ClientState -> IO ()
finishTx ref = writeIORef ref initState

addTxRequest :: Request -> ClientState -> ClientState
addTxRequest req state' = state' {txs = req : txs state'}

addTxRequestIO :: IORef ClientState -> Request -> IO ()
addTxRequestIO ref request = modifyIORef ref $ addTxRequest request
