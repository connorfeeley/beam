{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE GADTs #-}

module Database.Beam.Postgres.Test.Insert (tests) where

import           Data.Aeson
import           Data.ByteString                            (ByteString)
import           Data.Int
import           Data.UUID                                  (UUID, nil)
import qualified Data.UUID.V5                               as V5
import qualified Data.Vector                                as V
import           Database.Beam.Backend.SQL.BeamExtensions
import           Test.Tasty
import           Test.Tasty.HUnit

import           Database.Beam
import           Database.Beam.Backend.SQL.SQL92
import           Database.Beam.Migrate
import           Database.Beam.Postgres
import           Database.Beam.Postgres.Extensions.UuidOssp

import qualified Data.Text                                  as T
import           Database.Beam.Postgres.Full                (insertOnlyReturning,
                                                             onConflict)
import qualified Database.Beam.Postgres.Full                as Pg
import           Database.Beam.Postgres.Syntax              (PostgresInaccessible)
import           Database.Beam.Postgres.Test
import           Database.PostgreSQL.Simple                 (execute_)

tests :: IO ByteString -> TestTree
tests getConn = testGroup "Insertion Tests"
  [ testInsertOnConflict     getConn
  , testInsertOnlyOnConflict getConn
  , testUpsertFromSelect     getConn
  ]

testInsertOnConflict :: IO ByteString -> TestTree
testInsertOnConflict getConn = testCase "insertOnConflict" $
  withTestPostgres "insert_only_returning" getConn $ \conn -> do
    execute_ conn "CREATE TABLE insert_only_returning (id SERIAL NOT NULL UNIQUE PRIMARY KEY, value TEXT NOT NULL DEFAULT 'unknown')"
    inserted <- runBeamPostgres conn $ runInsertReturningList $
      insertOnConflict (tblInsertOnlyReturning insertOnlyReturningDb)
        ( insertExpressions
            [ InsertOnlyReturning 1 default_
            , InsertOnlyReturning 1 default_
            ]
        )
        anyConflict
        onConflictDoNothing
    assertEqual "inserted values"
      [ InsertOnlyReturning 1 "unknown"
      ] inserted
    inserted <- runBeamPostgresDebug putStrLn conn $ runInsertReturningList $
      insertOnConflict (tblInsertOnlyReturning insertOnlyReturningDb)
      -- Insert from result of SELECT.
        ( insertExpressions
            [ InsertOnlyReturning 1 default_
            ]
        )
      -- Only act when primary key conflicts.
      (conflictingFields primaryKey)
      -- Update conflicting fields of conflicting rows.
      (Pg.onConflictUpdateSet
        -- tbl is the old row, tblExcluded is the row proposed for insertion
        (\tbl tblExcluded -> mconcat
          [ _id    tbl <-. (_id    tblExcluded * 10)
          -- column reference "value" is ambiguous
          , _value tbl <-. concat_ [ (current_ . _value) tbl, " updated to ", _value tblExcluded ]
          ]
        )
      )
    assertEqual "inserted values"
      [ InsertOnlyReturning 10 "unknown updated to unknown"
      ] inserted


testInsertOnlyOnConflict :: IO ByteString -> TestTree
testInsertOnlyOnConflict getConn = testCase "insertOnlyOnConflict" $
  withTestPostgres "insert_only_returning" getConn $ \conn -> do
    -- Create table.
    execute_ conn "CREATE TABLE insert_only_returning (id SERIAL NOT NULL UNIQUE PRIMARY KEY, value TEXT NOT NULL DEFAULT 'unknown')"

    -- Insert test data.
    initialTestData <- runBeamPostgresDebug putStrLn conn $ runInsertReturningList $
      insertOnConflict (tblInsertOnlyReturning insertOnlyReturningDb)
        ( insertExpressions
            [ InsertOnlyReturning 1 default_
            , InsertOnlyReturning 2 (val_ "known")
            ]
        )
        anyConflict
        onConflictDoNothing
    assertEqual "inserted initial values"
      [ InsertOnlyReturning 1 "unknown"
      , InsertOnlyReturning 2 "known"
      ] initialTestData

    -- Select all rows from table and insert them again.
    inserted <- runBeamPostgresDebug putStrLn conn $ runInsertReturningList $
      insertOnlyOnConflict (tblInsertOnlyReturning insertOnlyReturningDb)
      -- Only insert 'id' column.
      _id
      -- Insert from result of SELECT.
      (insertFrom $ do
        c <- orderBy_ (asc_ . _id)
             (all_ (tblInsertOnlyReturning insertOnlyReturningDb))
        pure (_id c))
      -- Only act when primary key conflicts.
      (conflictingFields primaryKey)
      -- Update 'id' column of conflicting rows.
      (Pg.onConflictUpdateSet (\tbl tblExcluded -> mconcat [ _id    tbl <-. (_id    tblExcluded * 10) ]))
    assertEqual "inserted values"
      [ InsertOnlyReturning 10 "unknown"
      , InsertOnlyReturning 20 "known"
      ] inserted

    selected <- runBeamPostgresDebug putStrLn conn $ runSelectReturningList $ select $
      orderBy_ (asc_ . _id) $
      all_ (tblInsertOnlyReturning insertOnlyReturningDb)
    assertEqual "selected values"
      [ InsertOnlyReturning 10 "unknown"
      , InsertOnlyReturning 20 "known"
      ] selected

  where
    oc = Nothing :: Maybe (table (QExpr Postgres PostgresInaccessible) -> QExpr Postgres PostgresInaccessible Int)


testUpsertFromSelect :: IO ByteString -> TestTree
testUpsertFromSelect getConn = testCase "upsert results of query" $
  withTestPostgres "insert_only_returning" getConn $ \conn -> do
    -- Create table.
    execute_ conn "CREATE TABLE insert_only_returning (id SERIAL NOT NULL UNIQUE PRIMARY KEY, value TEXT NOT NULL DEFAULT 'unknown')"

    -- Insert test data.
    initialTestData <- runBeamPostgres conn $ runInsertReturningList $
      insertOnConflict (tblInsertOnlyReturning insertOnlyReturningDb)
        ( insertExpressions
            [ InsertOnlyReturning 1 default_
            , InsertOnlyReturning 2 (val_ "known")
            ]
        )
        anyConflict
        onConflictDoNothing
    assertEqual "inserted initial values"
      [ InsertOnlyReturning 1 "unknown"
      , InsertOnlyReturning 2 "known"
      ] initialTestData

    -- Select all rows from table and insert them again.
    inserted <- runBeamPostgresDebug putStrLn conn $ runInsertReturningList $
      insertOnlyOnConflict (tblInsertOnlyReturning insertOnlyReturningDb)
      -- Only insert 'id' column.
      _id
      -- Insert from result of SELECT.
      (insertFrom $ do
        c <- orderBy_ (asc_ . _id)
             (all_ (tblInsertOnlyReturning insertOnlyReturningDb))
        pure (_id c))
      -- Only act when primary key conflicts.
      (conflictingFields primaryKey)
      -- Update conflicting fields of conflicting rows.
      (Pg.onConflictUpdateSet
        -- tbl is the old row, tblExcluded is the row proposed for insertion
        (\tbl tblExcluded -> mconcat
          [ _id    tbl <-. (_id    tblExcluded * 10)
          -- column reference "value" is ambiguous
          , _value tbl <-. concat_ [ (current_ . _value) tbl, " updated to ", _value tblExcluded ]
          ]
        )
      )
    assertEqual "inserted values"
      [ InsertOnlyReturning 10 "unknown updated to unknown"
      , InsertOnlyReturning 20 "known updated to unknown"
      ] inserted

    selected <- runBeamPostgresDebug putStrLn conn $ runSelectReturningList $ select $
      orderBy_ (asc_ . _id) $
      all_ (tblInsertOnlyReturning insertOnlyReturningDb)
    assertEqual "selected values"
      [ InsertOnlyReturning 10 "unknown updated to unknown"
      , InsertOnlyReturning 20 "known updated to unknown"
      ] selected

  where
    oc = Nothing :: Maybe (table (QExpr Postgres PostgresInaccessible) -> QExpr Postgres PostgresInaccessible Int)


data InsertOnlyReturningT f = InsertOnlyReturning
  { _id    :: C f (SqlSerial Int32)
  , _value :: C f T.Text
  } deriving (Generic, Beamable)

deriving instance Show (InsertOnlyReturningT Identity)
deriving instance Eq (InsertOnlyReturningT Identity)

instance Table InsertOnlyReturningT where
  data PrimaryKey InsertOnlyReturningT f = WithDefaultsKey (C f (SqlSerial Int32))
    deriving (Generic, Beamable)
  primaryKey = WithDefaultsKey <$> _id

data InsertOnlyReturningDb entity where
  InsertOnlyReturningDb :: { tblInsertOnlyReturning :: entity (TableEntity InsertOnlyReturningT)
                           } -> InsertOnlyReturningDb entity
  deriving (Generic, Database Postgres)

insertOnlyReturningDb :: DatabaseSettings be InsertOnlyReturningDb
insertOnlyReturningDb = defaultDbSettings
