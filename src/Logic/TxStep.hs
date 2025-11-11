module Logic.TxStep where

import Data.Protocol.Types


type TxStep  = IO (IO (), STM RedisValue)

txStepFromSTM :: STM RedisValue -> TxStep
txStepFromSTM action = pure (pure (), action)

txStepFromA :: RedisValue -> TxStep
txStepFromA a =  pure (pure (), pure a)
