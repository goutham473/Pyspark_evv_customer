def textRead(spark, inputfile):
    return spark.read.text(inputfile)


def csvRead(spark, inputfile):
    return spark.read.option("header", "true").csv(inputfile)

