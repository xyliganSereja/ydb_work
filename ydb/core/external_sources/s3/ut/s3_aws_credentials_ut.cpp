#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>
#include <ydb/core/kqp/ut/olap/helpers/writer.h>
#include <ydb/core/kqp/ut/olap/helpers/query_executor.h>
#include <ydb/core/kqp/ut/olap/helpers/get_value.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/util/aws.h>
#include <ydb/core/wrappers/fake_storage.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>
#include <yql/essentials/utils/log/log.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/operation/operation.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/hook/hook.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/system/env.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NKikimr::NKqp::NFederatedQueryTest;
using namespace fmt::literals;

TString Exec(const TString& cmd) {
    std::array<char, 128> buffer;
    TString result;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
    if (!pipe) {
        throw std::runtime_error("popen() failed!");
    }
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe.get()) != nullptr) {
        result += buffer.data();
    }
    return result;
}

TString GetExternalPort(const TString& service, const TString& port) {
    auto dockerComposeBin = BinaryPath("library/recipes/docker_compose/bin/docker-compose");
    auto composeFileYml = ArcadiaFromCurrentLocation(__SOURCE_FILE__, "docker-compose.yml");
    auto result = StringSplitter(Exec(dockerComposeBin + " -f " + composeFileYml + " port " + service + " " + port)).Split(':').ToList<TString>();
    return result ? Strip(result.back()) : TString{};
}

void WaitBucket(std::shared_ptr<TKikimrRunner> kikimr, const TString& externalDataSourceName) {
    auto db = kikimr->GetQueryClient();
    for (size_t i = 0; i < 100; i++) {
        auto scriptExecutionOperation = db.ExecuteScript(fmt::format(R"(
            SELECT * FROM `{external_source}`.`/a/` WITH (
                format="json_each_row",
                schema(
                    key Utf8 NOT NULL,
                    value Utf8 NOT NULL
                )
            )
        )", "external_source"_a = externalDataSourceName)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

        NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
        if (readyOp.Metadata().ExecStatus == EExecStatus::Completed) {
            return;
        }
        Sleep(TDuration::Seconds(1));
    }
    UNIT_FAIL("Bucket isn't ready");
}

Y_UNIT_TEST_SUITE(S3AwsCredentials) {
    Y_UNIT_TEST(ExecuteScriptWithEqSymbol) {
        const TString externalDataSourceName = "/Root/external_data_source";
        auto s3ActorsFactory = NYql::NDq::CreateS3ActorsFactory();
        auto kikimr = MakeKikimrRunner(true, nullptr, nullptr, std::nullopt, s3ActorsFactory);
        kikimr->GetTestClient().GrantConnect("root1@builtin");
        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"(
            CREATE SECRET id WITH (value="minio");
            CREATE SECRET key WITH (value="minio123");
            CREATE EXTERNAL DATA SOURCE `{external_source}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{location}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_PATH="id",
                AWS_SECRET_ACCESS_KEY_SECRET_PATH="key",
                AWS_REGION="ru-central-1"
            );
            GRANT ALL ON `{external_source}` TO `root1@builtin`;
            )",
            "external_source"_a = externalDataSourceName,
            "location"_a = "localhost:" + GetExternalPort("minio", "9000") + "/datalake/"
            );
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        WaitBucket(kikimr, externalDataSourceName);
        auto db = kikimr->GetQueryClient();
        {
            auto scriptExecutionOperation = db.ExecuteScript(fmt::format(R"(
                SELECT * FROM `{external_source}`.`/a/` WITH (
                    format="json_each_row",
                    schema(
                        key Utf8 NOT NULL,
                        value Utf8 NOT NULL
                    )
                )
            )", "external_source"_a = externalDataSourceName)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
            UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

            NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
            UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
            TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
            UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

            TResultSetParser resultSet(results.ExtractResultSet());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);
            UNIT_ASSERT(resultSet.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "1");
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "trololo");
            UNIT_ASSERT(resultSet.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "2");
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "hello world");
        }

        {
            auto scriptExecutionOperation = db.ExecuteScript(fmt::format(R"(
                SELECT * FROM `{external_source}`.`/b/year=2023/` WITH (
                    format="json_each_row",
                    schema(
                        key Utf8 NOT NULL,
                        value Utf8 NOT NULL
                    )
                )
            )", "external_source"_a = externalDataSourceName)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
            UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

            NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
            UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
            TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
            UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

            TResultSetParser resultSet(results.ExtractResultSet());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);
            UNIT_ASSERT(resultSet.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "1");
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "trololo");
            UNIT_ASSERT(resultSet.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "2");
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "hello world");
        }

        {
            auto db = kikimr->GetQueryClient(NYdb::NQuery::TClientSettings().AuthToken("root1@builtin"));
            {
                auto scriptExecutionOperation = db.ExecuteScript(fmt::format(R"(
                    SELECT * FROM `{external_source}`.`/a/` WITH (
                        format="json_each_row",
                        schema(
                            key Utf8 NOT NULL,
                            value Utf8 NOT NULL
                        )
                    )
                )", "external_source"_a = externalDataSourceName)).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
                UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

                NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
                UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Failed, readyOp.Status().GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS_C(
                    readyOp.Status().GetIssues().ToString(),
                    "secrets `/Root/id`, `/Root/key` not found",
                    readyOp.Status().GetIssues().ToString()
                );
            }
            {
                const TString query = R"(
                    GRANT SELECT ROW ON `/Root/id` TO `root1@builtin`;
                    GRANT SELECT ROW ON `/Root/key` TO `root1@builtin`;
                )";
                auto result = session.ExecuteSchemeQuery(query).GetValueSync();
                UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            }
            {
                auto scriptExecutionOperation = db.ExecuteScript(fmt::format(R"(
                    SELECT * FROM `{external_source}`.`/a/` WITH (
                        format="json_each_row",
                        schema(
                            key Utf8 NOT NULL,
                            value Utf8 NOT NULL
                        )
                    )
                )", "external_source"_a = externalDataSourceName)).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
                UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

                NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
                UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
                TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
                UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

                TResultSetParser resultSet(results.ExtractResultSet());
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
                UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);
                UNIT_ASSERT(resultSet.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "1");
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "trololo");
                UNIT_ASSERT(resultSet.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "2");
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "hello world");
            }
            {
                auto scriptExecutionOperation = db.ExecuteScript(fmt::format(R"(
                    SELECT * FROM `{external_source}`.`/` WITH (
                        format="json_each_row",
                        schema(
                            key Utf8 NOT NULL,
                            value Utf8 NOT NULL
                        )
                    )
                )", "external_source"_a = externalDataSourceName)).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
                UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

                NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
                UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
                TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
                UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

                TResultSetParser resultSet(results.ExtractResultSet());
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
                UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 4);
                UNIT_ASSERT(resultSet.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "1");
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "trololo");
                UNIT_ASSERT(resultSet.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "2");
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "hello world");
            }

            {
                auto scriptExecutionOperation = db.ExecuteScript(fmt::format(R"(
                    INSERT INTO `{external_source}`.`exp_folder/` WITH (FORMAT = "csv_with_names")
                    SELECT "Hello, world!" AS Data
                )", "external_source"_a = externalDataSourceName)).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
                UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

                NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
                UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());             
            }
        }
        
    }

    Y_UNIT_TEST(TestInsertEscaping) {
        const TString externalDataSourceName = "/Root/external_data_source";
        auto s3ActorsFactory = NYql::NDq::CreateS3ActorsFactory();
        auto kikimr = MakeKikimrRunner(true, nullptr, nullptr, std::nullopt, s3ActorsFactory);

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"(
            CREATE SECRET id WITH (value="minio");
            CREATE SECRET key WITH (value="minio123");
            CREATE EXTERNAL DATA SOURCE `{external_source}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{location}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_PATH="id",
                AWS_SECRET_ACCESS_KEY_SECRET_PATH="key",
                AWS_REGION="ru-central-1"
            );
            )",
            "external_source"_a = externalDataSourceName,
            "location"_a = "localhost:" + GetExternalPort("minio", "9000") + "/datalake/"
            );

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        WaitBucket(kikimr, externalDataSourceName);

        auto db = kikimr->GetQueryClient();

        TString path = TStringBuilder() << "exp_folder/some_" << EscapeC(GetSymbolsString(' ', '~', "*?{}`")) << "\\`";

        {
            // NB: AtomicUploadCommit = "false" because in minio ListMultipartUploads by prefix is not supported
            auto scriptExecutionOperation = db.ExecuteScript(fmt::format(R"(
                PRAGMA s3.AtomicUploadCommit = "false";
                INSERT INTO `{external_source}`.`{path}/` WITH (FORMAT = "csv_with_names")
                SELECT * FROM `{external_source}`.`/a/` WITH (
                    format="json_each_row",
                    schema(
                        key Utf8 NOT NULL,
                        value Utf8 NOT NULL
                    )
                )
            )", "external_source"_a = externalDataSourceName, "path"_a = path)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
            UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

            NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
            UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
        }

        {
            auto scriptExecutionOperation = db.ExecuteScript(fmt::format(R"(
                SELECT * FROM `{external_source}`.`{path}/` WITH (
                    format="csv_with_names",
                    schema(
                        key Utf8 NOT NULL,
                        value Utf8 NOT NULL
                    )
                )
            )", "external_source"_a = externalDataSourceName, "path"_a = path)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
            UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

            NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
            UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
            TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
            UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

            TResultSetParser resultSet(results.ExtractResultSet());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);
            UNIT_ASSERT(resultSet.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "1");
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "trololo");
            UNIT_ASSERT(resultSet.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "2");
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "hello world");
        }

        {
            auto scriptExecutionOperation = db.ExecuteScript(fmt::format(R"(
                SELECT * FROM `{external_source}`.`{path}/` WITH (
                    format="csv_with_names",
                    `with_infer`="true",
                    `data.datetime.format`="%Y-%m-%dT%H-%M"
                )
            )", "external_source"_a = externalDataSourceName, "path"_a = path)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
            UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

            NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
            UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Failed, readyOp.Status().GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(readyOp.Status().GetIssues().ToString(), "parameter is not supported with type inference: data.datetime.format", readyOp.Status().GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(TieringSecretAccessCheck) {
        const TString externalDataSourceName = "/Root/tier1";
        const TString tableName = "/Root/olapStore/olapTable";
        const TString secretAccessKeyId = "aws-access-key-id";
        const TString secretAccessKey = "aws-secret-access-key";

        auto s3ActorsFactory = NYql::NDq::CreateS3ActorsFactory();
        
        TKikimrSettings runnerSettings;
        runnerSettings.WithSampleTables = false;
        runnerSettings.SetColumnShardAlterObjectEnabled(true);
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableColumnshardBool(true);
        featureFlags.SetEnableColumnStore(true);
        featureFlags.SetEnableTieringInColumnShard(true);
        featureFlags.SetEnableExternalDataSources(true);
        runnerSettings.SetFeatureFlags(featureFlags);
        runnerSettings.AppConfig.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        runnerSettings.AppConfig.MutableColumnShardConfig()->SetBulkUpsertRequireAllColumns(false);
        runnerSettings.SetS3ActorsFactory(s3ActorsFactory);
        
        TKikimrRunner kikimr(runnerSettings);
        
        Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->SetSecretKey("fakeSecret");
        
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_TIERING, NActors::NLog::PRI_DEBUG);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD_ACTUALIZATION, NActors::NLog::PRI_DEBUG);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD_BLOBS_TIER, NActors::NLog::PRI_DEBUG);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();
        
        auto tc = kikimr.GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        
        TLocalHelper olapHelper(kikimr);
        olapHelper.CreateTestOlapTable();
        
        const TString createSecretsQuery = fmt::format(R"(
            CREATE SECRET `{secret_access_key_id}` WITH (value="minio");
            CREATE SECRET `{secret_access_key}` WITH (value="minio123");
        )",
            "secret_access_key_id"_a = secretAccessKeyId,
            "secret_access_key"_a = secretAccessKey
        );

        auto result = session.ExecuteSchemeQuery(createSecretsQuery).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        
        const TString location = "http://fake.fake/olap-tier1";
        const TString createEDSQuery = fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `{external_source}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{location}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_PATH="{secret_access_key_id}",
                AWS_SECRET_ACCESS_KEY_SECRET_PATH="{secret_access_key}",
                AWS_REGION="ru-central-1"
            );
        )",
            "external_source"_a = externalDataSourceName,
            "location"_a = location,
            "secret_access_key_id"_a = secretAccessKeyId,
            "secret_access_key"_a = secretAccessKey
        );

        result = session.ExecuteSchemeQuery(createEDSQuery).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        
        const TString setTieringQuery = fmt::format(R"(
            ALTER TABLE `{table_name}` SET TTL Interval("P10D") TO EXTERNAL DATA SOURCE `{external_source}` ON timestamp
        )",
            "table_name"_a = tableName,
            "external_source"_a = externalDataSourceName
        );

        result = session.ExecuteSchemeQuery(setTieringQuery).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        
        WriteTestData(kikimr, tableName, 0, 3600000000, 1000);
        
        auto csController = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NKikimr::NOlap::TWaitCompactionController>();
        csController->WaitCompactions(TDuration::Seconds(10));
        csController->WaitActualization(TDuration::Seconds(10));
        
        Sleep(TDuration::Seconds(5));
        
        auto tableClient = kikimr.GetTableClient();
        auto selectQuery = TStringBuilder() << R"(
            SELECT
                TierName, SUM(ColumnRawBytes) AS RawBytes, SUM(Rows) AS Rows
            FROM `)" << tableName << R"(/.sys/primary_index_portion_stats`
            WHERE Activity == 1
            GROUP BY TierName
        )";
        
        auto rows = ExecuteScanQuery(tableClient, selectQuery);
        
        bool foundDefaultTier = false;
        for (auto&& row : rows) {
            auto tierName = GetUtf8(row.at("TierName"));
            if (tierName == "__DEFAULT") {
                foundDefaultTier = true;
            }
        }
        
        UNIT_ASSERT_C(foundDefaultTier, "Data should remain in default tier due to secret access error");
    }
}

Y_TEST_HOOK_BEFORE_RUN(InitAwsAPIForTiering) {
    NKikimr::InitAwsAPI();
}

Y_TEST_HOOK_AFTER_RUN(ShutdownAwsAPIForTiering) {
    NKikimr::ShutdownAwsAPI();
}

} // namespace NKikimr::NKqp
