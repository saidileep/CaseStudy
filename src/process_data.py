"""
__author__ = 'Sai Dileep'
__date__   = '19/03/2022'
__email__  = 'saidileep.tallapalli@gmail.com'
__desc__   = 'Module containing pyspark code to read input files based on config, perform ETL operations as per the case study provided
"""

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import Row, functions as F
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import IntegerType

import json
from utils import app_logger


def get_parameters():
    parsed_params_file_location = 'configs/params.json'

    try:
        with open('data.json') as json_file:
            params_body = json.load(json_file)
        app_logger.log('INFO', 'Successfully retrieved config file')
    except Exception as e:
        app_logger.log('ERROR', 'Error retrieving config file - ' + str(e))
        sys.exit(1)

    return params_body


if params_json is None: params_json = get_parameters()


class CaseStudy:
    def __init__(self):

        try:
            # Get Input File Paths
            self.units_input_path = params_json['InputFiles']['Units']
            self.charges_input_path = params_json['InputFiles']['Charges']
            self.damages_input_path = params_json['InputFiles']['Damages']
            self.persons_input_path = params_json['InputFiles']['Primary']
            self.restrictions_input_path = params_json['InputFiles']['Restrict']
            self.endorsements_input_path = params_json['InputFiles']['Endorse']

            # Get Output File Paths
            self.q1_output_path = params_json['OutputFiles']['Q1']
            self.q2_output_path = params_json['OutputFiles']['Q2']
            self.q3_output_path = params_json['OutputFiles']['Q3']
            self.q4_output_path = params_json['OutputFiles']['Q4']
            self.q5_output_path = params_json['OutputFiles']['Q5']
            self.q6_output_path = params_json['OutputFiles']['Q6']
            self.q7_output_path = params_json['OutputFiles']['Q7']
            self.q8_output_path = params_json['OutputFiles']['Q8']
            

            # Get use case specific parameters
            self.top_n_states = params_json['TopStatesN']
            self.top_n_colors = params_json['TopColorsN']

            # Define logging configs
            self.component_type = 'ETL_PROCESS'
            self.component_name = 'case_study'
            self.activity_id = str(uuid.uuid4())
            self.event_start_date_time = str(datetime.datetime.now())

        except Exception as e:
            app_logging.log('ERROR', 'Error Initializing configs- ' + str(e))
            sys.exit(1)



    def write_to_csv(df, file_path):
        """write data from given dataframe to csv file
        :param df: Input dataframe which needs to be written out
        :file_path: output path to write the data
        :return: Spark DataFrame.
        """

        df.write.csv(file_path)

    def get_dataframe(spark, file_path):
        """Load data from input csv files and deduplicates the data
        :param spark: Spark session object.
        :param df: Spark session object.
        :out_path: type of the input file to lookup from config
        :return: None
        """

        df = (
            spark
            .read
            .option('header',True)
            .csv(file_path))
            .dropDuplicates()

        return df

    def getKeywordFilterString(col_list):
        """create a query string for filtering damaged crashes
        :param col_list: list of columns to be used for constructing query
        :return: query string which can be passed to spark dataframe filter function.
        """
        filter_string = ''
        for col_name in col_list:
            if filter_string == '':
                filter_string = """{} like '%DAMAGED%'""".format(col_name)
            else:
                filter_string = filter_string + """ OR {} like '%DAMAGED%'""".format(col_name)
        return filter_string

    def getAdditionalColumns(col_list, df):
        """Appends additional type casted columns indicating damage level
        :param col_list: list of columns to be used for constructing query
        :df: input dataframe to which columns need to be appended
        :return: Input DataFrame with additional columns equal to number of items in col_list.
        """
        for col_name in col_list:
            df = df.withColumn('{}_lvl'.format(col_name),F.col(col_name).substr(9,1).cast(IntegerType()))
        return df

    def getLevelFilterString(col_list, level):
        """create a query string for filtering damage level n
        :param col_list: list of columns to be used for constructing query
        :param level: minimum damage level to be used in filter condition
        :return: query string which can be passed to spark dataframe filter function.
        
        """
        level_filter_string = ''
        for col_name in col_list:
            if level_filter_string == '':
                level_filter_string = """{}_lvl > {}""".format(col_name,level)
            else:
                level_filter_string = level_filter_string + """ OR {}_lvl >{}""".format(col_name,level)
        return level_filter_string

    def getTopNVehicleColors(units_df,num):
        """Returns top n colors with more crashes
        :param units_df: input dataframe
        :param num: number of colors to return
        :return: list of strings with top n colors
        """
        crash_count_by_color_df = units_df.groupBy("VEH_COLOR_ID").count().orderBy(F.col("count").desc()).limit(num)
        top_n_vehicle_colors = [row[0] for row in crash_count_by_color_df.select('VEH_COLOR_ID').collect()]
        return top_n_vehicle_colors

    def getTopNStates(units_df,num):
        """returns top n states with more accidents
        :param units_df: input dataframe
        :param num: number of states to return
        :return: list of strings with top n states
        """
        crash_count_by_state_df = units_df.groupBy("VEH_LIC_STATE_ID").agg(countDistinct("CRASH_ID").alias("ct")).orderBy(F.col("ct").desc()).limit(num)
        top_n_states = [row[0] for row in crash_count_by_state_df.select('VEH_LIC_STATE_ID').collect()]
        return top_n_states

    def execute(self):
        """Executes the entire logic by using helper functions
            and configs/variables from class object
        
        """
        app_logging.log('INFO', '--JOB START--')

        # Creating spark session
        spark = SparkSession.builder \
            .master("local[1]") \
            .appName("BCG_CaseStudy") \
            .getOrCreate()

        # Creating all the required input dataframes
        charges_df = getDataFrame(spark,self.charges_input_path)
        units_df = getDataFrame(spark,self.units_input_path)
        damages_df = getDataFrame(spark,self.damages_input_path)
        person_df = getDataFrame(spark,self.persons_input_path)
        restrictions_df = getDataFrame(spark,self.restrictions_input_path)
        endorsements_df = getDataFrame(spark,self.endorsements_input_path)

        #Q1
        q1_res_df = person_df.filter((person_df.PRSN_GNDR_ID=='MALE') & (person_df.DEATH_CNT==1)).select("CRASH_ID").distinct().count()
        write_to_csv(q1_res_df,self.q1_output_path)


        #Q2
        q2_res_df = units_df.filter(units_df.VEH_BODY_STYL_ID.contains('MOTORCYCLE')).select("VIN").distinct().count()
        write_to_csv(q2_res_df,self.q2_output_path)

        

        #Q3
        filtered_person_df = person_df.filter(person_df.PRSN_GNDR_ID=='FEMALE')
        grouped_df = filtered_person_df.groupBy("DRVR_LIC_STATE_ID").agg(countDistinct("CRASH_ID").alias("ct"))
        q2_res_df = grouped_df.orderBy(F.col("ct").desc()).select("DRVR_LIC_STATE_ID").limit(1)
        write_to_csv(q3_res_df,self.q3_output_path)

        

        #Q4
        make_crash_counts_df = units_df.filter((units_df.TOT_INJRY_CNT>0) | (units_df.DEATH_CNT>0)).groupBy("VEH_MAKE_ID").count()
        ranked_df = make_crash_counts_df.withColumn('crash_count_order', F.dense_rank().over(Window.orderBy(F.col("count").desc())))
        q4_res_df = ranked_df.filter((ranked_df.crash_count_order >=5) & (ranked_df.crash_count_order <=15)).select("VEH_MAKE_ID")
        write_to_csv(q4_res_df,self.q4_output_path)

        

        #Q5
        selected_units_df = units_df.select("CRASH_ID", "UNIT_NBR","VEH_BODY_STYL_ID").distinct()
        joined_df = person_df.join(selected_units_df, (person_df.CRASH_ID == selected_units_df.CRASH_ID) & (person_df.UNIT_NBR == selected_units_df.UNIT_NBR))
        grouped_df = joined_df.groupBy("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID").count()
        ranked_df = grouped_df.withColumn('crash_count_order', F.row_number().over(Window.partitionBy("VEH_BODY_STYL_ID").orderBy(F.col("count").desc())))
        q5_res_df = ranked_df.filter(ranked_df.crash_count_order == 1).select("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID")
        write_to_csv(q5_res_df,self.q5_output_path)

        

        #Q6
        alcohol_charges_df = charges_df.filter(charges_df.CHARGE.contains('ALCOHOL'))
        selected_prsn_df = person_df.select("CRASH_ID", "UNIT_NBR","PRSN_NBR","DRVR_ZIP").distinct()
        joined_df = alcohol_charges_df.join(selected_prsn_df, (alcohol_charges_df.CRASH_ID == selected_prsn_df.CRASH_ID) & (alcohol_charges_df.UNIT_NBR == selected_prsn_df.UNIT_NBR) & (alcohol_charges_df.PRSN_NBR == selected_prsn_df.PRSN_NBR))
        q6_res_df = joined_df.groupBy("DRVR_ZIP").count().orderBy(F.col("count").desc()).select("DRVR_ZIP").limit(5)
        write_to_csv(q6_res_df,self.q6_output_path)

        

        #Q7
        damage_cols = [x for x in units_df.schema.names if x.startswith("VEH_DMAG_SCL_")]
        # generating list of select columns
        select_cols = ['CRASH_ID'] + damage_cols
        # filter the dataframe with vehicles which got insurance
        pre_df = units_df.filter(units_df.FIN_RESP_TYPE_ID.contains('INSURANCE')).select(select_cols)
        filter_string = getKeywordFilterString(damage_cols)
        only_damaged_units_df =pre_df.filter(filter_string)
        add_damage_cols_df = getAdditionalColumns(damage_cols,only_damaged_units_df)
        #display(add_damage_cols_df)
        level_filter_string = getLevelFilterString(damage_cols,4)
        threshold_damage_units_df = add_damage_cols_df.filter(level_filter_string)
        #display(threshold_damage_units_df)
        sel_units_df = threshold_damage_units_df.select("CRASH_ID").distinct()
        sel_damages_df= damages_df.select("CRASH_ID").distinct()
        joined_df = sel_units_df.join(sel_damages_df,(sel_units_df.CRASH_ID == sel_damages_df.CRASH_ID))
        q7_res_df = joined_df.toDF("CRASH_ID", "CRASH_ID_2").select("CRASH_ID").distinct().count()
        write_to_csv(q7_res_df,self.q7_output_path)

        
        #Q8
        only_speeding_charges_crashes_df = charges_df.filter(charges_df.CHARGE.contains('SPEED')).select("CRASH_ID").distinct()
        #display(speeding_charges_df)

        only_licensed_drivers_crashes_df = person_df.filter(person_df.DRVR_LIC_CLS_ID.contains('CLASS')).select("CRASH_ID").distinct()
        #display(only_licensed_drivers_crashes_df)

        top_10_vehicle_colors = getTopNVehicleColors(units_df,10)
        #print(top_10_vehicle_colors)

        top_25_states= getTopNStates(units_df,25)
        #print(top_25_states)

        filtered_units_df = units_df.filter((units_df.VEH_COLOR_ID.isin(top_10_vehicle_colors)) & (units_df.VEH_LIC_STATE_ID.isin(top_25_states)))
        selected_units_df = filtered_units_df.select("CRASH_ID", "UNIT_NBR","VEH_MAKE_ID") 
        #display(selected_units_df)

        speeding_charges_units_joined_df = selected_units_df.join(only_speeding_charges_crashes_df, (selected_units_df.CRASH_ID == only_speeding_charges_crashes_df.CRASH_ID)).join(only_licensed_drivers_crashes_df,(only_licensed_drivers_crashes_df.CRASH_ID == selected_units_df.CRASH_ID))

        crash_count_by_vehicle_make_df = speeding_charges_units_joined_df.groupBy("VEH_MAKE_ID").count().orderBy(F.col("count").desc())
        q8_res_df = crash_count_by_vehicle_make_df.select("VEH_MAKE_ID").limit(5)
        write_to_csv(87_res_df,self.q8_output_path)
        
        app_logging.log('INFO','Data processing complete!!')
        app_logging.log('INFO', '--JOB END--')

        return 'Successfully processed the data!'


# Entry point for our spark application
if __name__ == '__main__':
    case_study = CaseStudy()
    response =  case_study.execute()
