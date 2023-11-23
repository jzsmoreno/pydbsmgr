from pydbsmgr.utils.sql_functions import FileToSQL


def test_bulk_insert_from_csv(
    connection_string,
    file_name,
    db_table_name,
    sas_str,
    storage_account,
    container_name,
    storage_connection_string,
):
    file_sql = FileToSQL(
        connection_string=connection_string,
    )

    # file_sql.drop_dropables("UploadCSV", "UploadCSV", True)
    res = file_sql.bulk_insert_from_csv(
        file_name=file_name,
        db_table_name=db_table_name,
        sas_str=sas_str,
        storage_account=storage_account,
        container_name=container_name,
        storage_connection_string=storage_connection_string,
    )
    assert res == True


if __name__ == "__main__":
    test_bulk_insert_from_csv(
        connection_string="Driver={ODBC Driver 18 for SQL Server};Server=tcp:gendevsrv01.database.windows.net,1433;Database=devpeopleoptidbsrv01;Uid=sqladminsandbox;Pwd=tzmRV+Pm4rSA;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;",
        file_name="Rotacion/TB_BI_bimboAusentismo.csv",
        db_table_name="Testing_Bulk_BI_bimboAusentismo",
        sas_str="sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-11-23T05:20:00Z&st=2023-11-22T21:20:00Z&spr=https&sig=J0HZuRWa%2BTeLX2T9tz0AH4PHSbfTZ%2FGlvQgoHmnkNKk%3D",
        storage_account="bimbosa",
        container_name="processed",
        storage_connection_string="DefaultEndpointsProtocol=https;AccountName=bimbosa;AccountKey=5vrOk5G7y0D5zK1K/EKjKuMCJKBd9H1sCoWZvybiKXtlIFmpmpK2KQ0kNmGcdh105doHk4Zpbl16+AStZKo5CA==;EndpointSuffix=core.windows.net",
    )
