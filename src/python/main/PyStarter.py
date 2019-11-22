from conf.appConf import AppConf
from process.ProcessEvvCustomer import ProcessEvvCustomer
from util.sparkSessionProvider import createSparkSession
import sys

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Need 4 arguments")
        sys.exit(-1)

appConfig = AppConf(str(sys.argv[1]), str(sys.argv[2]), str(sys.argv[3]), str(sys.argv[4]))

spark = createSparkSession()

try:
    process = ProcessEvvCustomer()
    process.process_evv_cust_exec(spark, appConfig)

except Exception as e:
    raise e


