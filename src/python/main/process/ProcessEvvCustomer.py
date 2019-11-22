#from src.python.main.conf.appConf import AppConf
from read.read_tables_file import *
from pyspark.sql.functions import *
from write.write_tgt_file import *
from pyspark.sql.types import DataType
from datetime import date
from pyspark.sql.types import StringType
class ProcessEvvCustomer(object):

    def process_evv_cust_exec(self, spark, app_conf):
        readSrcTablefile = textRead(spark, app_conf.inputpath + "/" + app_conf.inputfilename).collect()
        readonebyone = [str(row['value']) for row in readSrcTablefile]
        readdata = [csvRead(spark, app_conf.inputpath + "/" + row) for row in readonebyone]
        readtoccabc = readdata[0].withColumn("CCABC", expr("CABC")).withColumn('ICABC', expr("IABC")).cache()
        readtoccasetyperabc = readdata[1].withColumn('CTRCABC', expr("CABC")).withColumn('CTRIABC', expr('IABC'))
        readtocearningsrecabc = readdata[2].withColumn('ERCABC', expr("CABC")).withColumn('ERIABC', expr('IABC'))
        readtococcupatabc = readdata[3].withColumn('OCABC', expr('CABC')).withColumn('OIABC', expr('IABC'))
        readtococcupationcabc = readdata[4].withColumn('OCCABC', expr('CABC')).withColumn('OCIABC', expr('IABC'))
        readtocpartycaserabc = readdata[5].withColumn('PCRCABC', expr('CABC')).withColumn('PCRIABC', expr('IABC'))
        readdata[6].select('DateOfBirthABC').show(10,False) #printSchema()
        readtocperabc = readdata[6].withColumn('PCABC', expr('CABC')).withColumn('PIABC', expr('IABC')).withColumn('FMTDDateOfBirthABC', to_date(from_unixtime(unix_timestamp(expr('DateOfBirthABC'), 'MM/dd/yyyy HH:mm:ss'))))
        readtocperabc.select('FMTDDateOfBirthABC').show(10)

        readtolcclaimoccupatabc = readdata[7].withColumn('COCABC', expr('CABC')).withColumn('COIABC', expr('IABC'))
        readtolcontrabc = readdata[8].withColumn('CONCABC', expr('CABC')).withColumn('CONIABC', expr('IABC'))
        readtolincomesouabc = readdata[9].withColumn('IOCABC', expr('CABC')).withColumn('IOCIABC', expr('IABC'))
        readtolpartydetaabc = readdata[10].withColumn('PDCABC', expr('CABC')).withColumn('PDIABC', expr('IABC'))
        readtolpaymentprefereabc = readdata[11].withColumn('PPCABC', expr('CABC')).withColumn('PPIABC', expr('IABC'))
        readtocorganizatabc = readdata[12].withColumn('ORGCABC', expr('CABC')).withColumn('ORGIABC', expr('IABC'))
        readtocpartycontractrabc = readdata[13].withColumn('PCCABC', expr('CABC')).withColumn('PCIABC', expr('IABC'))
        readtoccontractsabc = readdata[14].withColumn('CSCABC', expr('CABC')).withColumn('CSIABC', expr('IABC'))
        readtodomaininstaabc = readdata[15].withColumn('DOMICABC', expr('CABC')).withColumn('DOMIIABC', expr('IABC'))
        readtocpointofcontabc = readdata[16].withColumn('POCCABC', expr('CABC')).withColumn('POCIABC', expr('IABC'))
        readlkpgroupindicatabc = readdata[17]
        readmtrorabc = readdata[18]
        #cust_age_udf = udf(cal_age, IntegerType())
        joinPersonInfo = personSelectCol(
            #personInfo(readtocperabc, readtococcupatabc, readtocearningsrecabc, readtocorganizatabc, readtolpartydetaabc, readtolpaymentprefereabc, readtodomaininstaabc, readtococcupationcabc))
             personInfo(readtocperabc, readtococcupatabc, readtocearningsrecabc, readtocorganizatabc, readtolpartydetaabc, readtolpaymentprefereabc))
                        #, readtolpartydetaabc))
        joinPersonInfo.show()
        filewrite = filewritecsv(joinPersonInfo)
        showdf = filewrite.show()
        csvWrite(filewrite, app_conf.outputpath + "/" + app_conf.outputfilename)


def personInfo(readtocperabc, readtococcupatabc, readtocearningsrecabc, readtocorganizatabc, readtolpartydetaabc,readtolpaymentprefereabc):
               #, readtocearningsrecabc, readtocorganizatabc, readtolpartydetaabc):
        return readtocperabc.join(readtococcupatabc, (readtocperabc['PCABC'] == readtococcupatabc['OCABC']) & (readtocperabc['PIABC'] == readtococcupatabc['OIABC']), "left")\
            .join(readtocearningsrecabc, (readtococcupatabc['OCABC'] == readtocearningsrecabc['C_OCOCCPTN_EARNINGSRECORABC']) & (readtococcupatabc['OIABC'] == readtocearningsrecabc['I_OCOCCPTN_EARNINGSRECORABC']), "left")\
            .join(readtocorganizatabc, (readtococcupatabc['C_OCPRTY_EMPLOYEEOCCUPABC'] == readtocorganizatabc['CABC']) & (readtococcupatabc['I_OCPRTY_EMPLOYEEOCCUPABC'] == readtocorganizatabc['IABC']), "left")\
            .join(readtolpartydetaabc, (readtocperabc['PCABC'] == readtolpartydetaabc['C_OCPRTY_PARTYABC']) & (readtocperabc['PIABC'] == readtolpartydetaabc['I_OCPRTY_PARTYABC']))\
            .join(readtolpaymentprefereabc, (readtolpartydetaabc['PDCABC'] == readtolpaymentprefereabc['C_PRTDTLS_PAYMENTPREFERABC']) & (readtolpartydetaabc['PDIABC'] == readtolpaymentprefereabc['I_PRTDTLS_PAYMENTPREFERABC']), "left")
#            .join(readtodomaininstaabc, ['EXTPAYMENTROUTINGABC'] == readtodomaininstaabc['DOMIIABC'], "left")\
#            .join(readtococcupationcabc, readtococcupatabc['I_OCCUPCDE_OCCUPATIONSABC'] == readtococcupationcabc['OCIABC'],"left")


def cal_age(dob):
    today = date.today()
    x = 0
    year_diff = today.year - dob.year
    print('year difference:' + str(year_diff))
    dob_year = int(dob.year)
    print('dob year: ' + str(dob_year))
    for i in range(1, year_diff):
        if int(dob_year) % 4 == 0:
            x = x + 1
            dob_year = int(dob_year) + 1
            print('print x: ' + str(x))
        else:
            print('Hello')
            dob_year = int(dob_year) + 1
            print('dob_year value' + str(dob_year))
    print('dob-today: ' + str(dob - today))
    if dob < today:
        print("dob date" + str(dob))
        print("today date" + str(today))

        days = str(today - dob).replace(' days, 0:00:00', '')
        days_int = int(days)
        return str((days_int + x) / 365)

    else:
        print("dob is greater than todays date")

        return ""
#spark.udf.register("customerAge", cal_age)
cust_age_udf = udf(cal_age, StringType())

def personSelectCol(personInf):
        return personInf.select(personInf['PCABC'], personInf['PIABC'], personInf['NatlnsNoABC'].alias('CSTMR_SN')
                                ,personInf['FIRSTNAMESABC'].alias('CSTMR_FST_NAM'), personInf['INITIALSABC'].alias('CSTMR_MI_NAM')
                                ,cust_age_udf(personInf['FMTDDateOfBirthABC']).alias('cust_age'))
        #,personInf['FMTDDateOfBirthABC'])



def filewritecsv(personSelectCol):
        return personSelectCol.select(personSelectCol.PCABC, personSelectCol.PIABC, personSelectCol.CSTMR_SN, personSelectCol.CSTMR_FST_NAM, personSelectCol.cust_age
                                      )





