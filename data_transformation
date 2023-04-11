from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

import pandas as pd

from scipy.stats import chi2_contingency
from scipy.stats import chisquare
from itertools import permutations
from statsmodels.sandbox.stats.multicomp import multipletests
import numpy as np
import os

##########################################################
## generate input data for flow comparison
##########################################################

def gen_initial_data(raw_df, target_action, start_date, end_date, window_period, min_duration):

    spark = SparkSession.builder.master("local[*]").getOrCreate()
    sc = spark.sparkContext

    # load raw data
    start_df = raw_df \
            .withColumn('event_type',
                        when(col('event').isin(target_action), lit('action')).otherwise(lit('visit'))) \
            .withColumn('start_time',  to_timestamp(col('start_time'))) \
            .withColumn('end_time',  to_timestamp(col('end_time'))) \
            .withColumn('duration_second',
                       col('end_time').cast(LongType())-col('start_time').cast(LongType())) \
            .withColumn('date_id', to_date(col('start_time'))) \
            .where(col('date_id').between(start_date, end_date))
    
    # calculated session_id
    start_df = start_df \
        .withColumn('session_id', lag('end_time',1).over(Window.partitionBy('id').orderBy('end_time'))) \
        .withColumn('session_id', col('end_time').cast(LongType())-col('session_id').cast(LongType())) \
        .withColumn('session_id', when(col('session_id')<=window_period,lit(0)).otherwise(lit(1))) \
        .withColumn('session_id', sum('session_id').over(Window.partitionBy('id').orderBy('end_time'))) \
        .withColumn('session_id', concat_ws("_",col('id'),col('session_id')))

    # create visit df and action df

    visit_raw = start_df.where(col('event_type').isin('visit') & (col('duration_second')>min_duration))

    action_raw = start_df.where(col('event_type').isin('action')) \
                         .selectExpr('id','date_id','start_time as action_time')
    
    # deal with visit dataframe

    partition_list = ['id','session_id','event','group_id']

    visit_df = visit_raw \
    .withColumn('seq_id',
                row_number().over(Window.partitionBy(partition_list[:2]).orderBy('start_time'))) \
    .withColumn('content_seq_id',
                row_number().over(Window.partitionBy(partition_list[:3]).orderBy('start_time'))) \
    .withColumn('group_id', col('seq_id')-col('content_seq_id'))\
    .withColumn('pg_duration', sum('duration_second').over(Window.partitionBy(partition_list))) \
    .withColumn('pg_start_time', min('start_time').over(Window.partitionBy(partition_list))) \
    .withColumn('pg_end_time', max('end_time').over(Window.partitionBy(partition_list))) \
    .select('id','event','session_id','pg_duration','pg_start_time','pg_end_time').distinct() \

    visit_df = visit_df \
    .withColumn('session_id',lag('pg_end_time',1).over(Window.partitionBy('id').orderBy('pg_end_time'))) \
    .withColumn('session_id', col('pg_end_time').cast(LongType())-col('session_id').cast(LongType())) \
    .withColumn('session_id', when(col('session_id')<=window_period,lit(0)).otherwise(lit(1))) \
    .withColumn('session_id', sum('session_id').over(Window.partitionBy('id').orderBy('pg_end_time'))) \
    .withColumn('session_id', concat_ws("_",col('id'),col('session_id')))
    
    # join visit with target action

    action_merge = visit_df \
        .join(action_raw, ['id'], 'left').distinct() \
        .withColumn('action_rn', rank().over(Window.partitionBy('id','session_id').orderBy(desc('action_time')))) \
        .where(col('action_rn')==1).drop('action_rn') \
        .withColumn('action_period', col('action_time').cast(LongType())-col('pg_end_time').cast(LongType())) \
        .withColumn('in_window', col('action_period').between(0,window_period)) \
        .withColumn('convert_yn',max(when(col('in_window')==True, lit(1))).over(Window.partitionBy('id','session_id'))) \
        .withColumn('convert_yn', coalesce(col('convert_yn'), lit(0))) \
        .withColumn('drop_record',when((col('convert_yn')==1) & (col('in_window')==False), lit('drop')).otherwise(lit('keep'))) \
        .withColumn('action_time', when(col('in_window')==False, lit(None)).otherwise(col('action_time'))) \
        .withColumn('action_period', when(col('in_window')==False, lit(None)).otherwise(col('action_period'))) \
        .where(col('drop_record')=='keep').drop('drop_record')
    
    order_df = action_merge \
        .withColumn('page_order',rank().over(Window.partitionBy('id','session_id').orderBy('pg_start_time')))

    seq_num = order_df.where(col('convert_yn')==1).agg(max('page_order')).collect()[0][0]
    
    # Calculated Idle Time
    idle_df = order_df \
      .withColumn('next_start_time', 
                  lead('pg_start_time',1).over(Window.partitionBy('id','session_id').orderBy('pg_start_time'))) \
      .withColumn('lag_duration_second', 
                  col('next_start_time').cast(LongType())-col('pg_end_time').cast(LongType())) \
      .where(col('lag_duration_second').isNotNull()) \
      .select('id','session_id','page_order','lag_duration_second') \
      .withColumnRenamed('lag_duration_second','pg_duration') \
      .withColumn('event', lit('idle time')) \
      .withColumn('page_order', col('page_order')+0.5)

    final_all_order = order_df.unionByName(idle_df, True) \
        .withColumn('convert_yn',
                    last(col('convert_yn'),True).over(Window.partitionBy('id','session_id'))) \
        .withColumn('exclude_yn'
                    ,when((col('convert_yn')==0) & (col('page_order')<=seq_num), lit('include'))
                    .when(col('convert_yn')==1, lit('include'))) \
        .where(col('exclude_yn').isin('include')).drop('exclude_yn') \
        .withColumn('all_event',
                    collect_list(when(col('event')!='idle time', col('event')))
                    .over(Window.partitionBy('id','session_id'))) \
        .withColumn('total_seq', size(col('all_event'))) \
        .withColumn('main_flow', array_join('all_event',"→")) \
        .withColumn('unique_id', col('id')) \
        .withColumn('action_flag', col('convert_yn')==1) \
        .withColumn('first_action_dt', min('pg_start_time').over(Window.partitionBy('id','session_id'))) \
        .withColumn('action_duration', col('action_time').cast(LongType())-col('first_action_dt').cast(LongType()))
    
    return final_all_order

def gen_flow_comparison_result(final_all_order):
    
    # generate flow dataframe
    
    flow_df = final_all_order \
        .groupBy('main_flow','page_order') \
        .agg(first('event').alias('page')
            ,avg('pg_duration').alias('duration')
            ,countDistinct('unique_id').alias('frequency')
            ,avg(when(col('action_flag')==True, col('action_duration'))).alias('action_time')
            ,avg(when(col('action_flag')==True, col('pg_duration'))).alias('convert_duration')
            ,avg(when(col('action_flag')==False, col('pg_duration'))).alias('unconvert_duration')) \
        .orderBy(desc('frequency')) \
        .withColumn('temp_order', when(col('page')=='idle time', lit(None)).otherwise(col('page_order'))) \
        .withColumn('next_page', when(col('page')!='idle time', lead('page',1).over(Window.partitionBy('main_flow').orderBy('temp_order')))) \
        .drop('temp_order') \
        .toPandas()
    
    # generate conversion dataframe
    
    conversion_df = final_all_order \
        .groupBy('main_flow') \
        .pivot('action_flag', [True,False]) \
        .agg(countDistinct('unique_id').alias('frequency')) \
        .na.fill(0) \
        .withColumn('total', col('true')+col('false')) \
        .withColumn('convert_rate', round(col('true')/col('total'),3)) \
        .toPandas()
    
    # generate comparison result
    
    compare_df = conversion_df[(conversion_df['true']>0) & (conversion_df['total']>50)]

    c_event = compare_df['main_flow'].unique().tolist()
    compare_pd = compare_df.drop('total',axis=1)

    compare_pd['true'] = compare_pd['true'].astype('float')
    compare_pd['false'] = compare_pd['false'].astype('float')

    all_combinations = list(permutations(c_event, 2))

    pvals = []
    srm_pval_li = []
    result_li = []

    for comb in all_combinations:
        # flow comparison
        new_df = compare_pd[(compare_pd['main_flow'] == comb[0]) | (compare_pd['main_flow'] == comb[1])]
        chi2, p, dof, ex = chi2_contingency(new_df[['true','false']], correction=False)
        pvals.append(p)

        # SRM test
        new_df['total'] = new_df['true']+new_df['false']
        observed = new_df['total'].to_list()
        expected = [new_df['total'].sum()/2, new_df['total'].sum()/2]

        srm_pval = chisquare(observed,f_exp=expected)[1]
        srm_pval_li.append(srm_pval)

    reject_list, corrected_p_vals = multipletests(pvals, method='bonferroni')[:2]

    for p_val, corr_p_val, reject, comb, srm_pval_li in zip(pvals, corrected_p_vals, reject_list, all_combinations,srm_pval_li):
        result_li.append([comb[0],comb[1],p_val,corr_p_val,reject,srm_pval_li])

    result = pd.DataFrame(result_li, columns=['main_flow','compared_flow','pval','pval_correct','reject','srm_pval'])


    main_cvr = conversion_df.rename(columns={'convert_rate':'main_cvr',
                                         'true':'main_convert',
                                         'total':'main_total'})[['main_flow','main_cvr','main_convert','main_total']]

    compare_cvr = conversion_df.rename(columns={'main_flow':'compared_flow',
                                              'convert_rate':'compared_cvr',
                                              'true':'compared_convert',
                                              'total':'compared_total'})[['compared_flow','compared_cvr','compared_convert','compared_total']]

    prep_result = result \
    .merge(main_cvr, on='main_flow', how='left') \
    .merge(compare_cvr, on='compared_flow', how='left')

    prep_result['diff'] = prep_result['main_cvr']-prep_result['compared_cvr']
    prep_result['confidence'] = 1-prep_result['pval_correct']
    prep_result['value'] = np.round(np.where(prep_result['diff']<0, prep_result['confidence']*-1, prep_result['confidence']),4)
    prep_result['srm_caution'] = np.where(prep_result['srm_pval'] < 0.05, 'significantly SRM', 'not found SRM')

    prep_result = prep_result[['main_flow','compared_flow','pval_correct','confidence','diff','value','main_cvr','compared_cvr','srm_pval','srm_caution']]

    sorting = flow_df[['main_flow','frequency']].drop_duplicates()

    conf = prep_result.merge(sorting.rename(columns={'frequency':'main_frequency'}), on='main_flow', how='left') \
          .merge(sorting.rename(columns={'main_flow':'compared_flow','frequency':'compared_frequency'}), on='compared_flow', how='left')

    source = flow_df[flow_df['frequency']>50]
    source1 = source['main_flow'].unique().tolist()
    confident1 = conf[conf['main_flow'].isin(source1)]

    source2 = confident1['main_flow'].unique().tolist()
    confident_df = confident1[confident1['main_flow'].isin(source2)]
    
    # prepare file to write
    
    flow_conversion = flow_df.merge(conversion_df, on='main_flow', how='inner')
    select_flow = set(flow_conversion['main_flow'].to_list())
    confident_compare = confident_df[confident_df['main_flow'].isin(select_flow) & confident_df['compared_flow'].isin(select_flow)]
    
    compared_info = confident_compare[['main_flow','compared_flow','pval_correct','confidence','diff','value','srm_pval','srm_caution']]

    flow_info = flow_conversion[flow_conversion['frequency']>50][['main_flow','frequency','convert_rate']].drop_duplicates()

    main_cvr = flow_info.rename(columns={'convert_rate':'main_cvr',
                                         'frequency':'main_frequency'}) \
                [['main_flow','main_cvr','main_frequency']]

    compare_cvr = flow_info.rename(columns={'main_flow':'compared_flow',
                                            'convert_rate':'compared_cvr',
                                            'frequency':'compared_frequency'}) \
                [['compared_flow','compared_cvr','compared_frequency']]


    flow_select = flow_conversion[flow_conversion['frequency']>50]['main_flow'].unique()
    universe_combination = list(permutations(flow_select, 2))
    uni_flow = pd.DataFrame(universe_combination, columns=['main_flow','compared_flow'])
    
    
    ##### export file #####
    
    export_flow_conversion = flow_conversion.copy()

    export_confident_final = uni_flow \
            .merge(main_cvr, on=['main_flow'], how='left') \
            .merge(compare_cvr, on=['compared_flow'], how='left') \
            .merge(compared_info, on=['main_flow','compared_flow'], how='left')
    
    export_model_final = flow_conversion[['main_flow','false','true']].drop_duplicates()   

    
    return export_flow_conversion, export_confident_final, export_model_final

def main(import_df,target_action, start_date, end_date,window_period,min_duration,target_dir):
    
    os.mkdir(target_dir)

    final_all_order = gen_initial_data(import_df, target_action, start_date, end_date,window_period,min_duration)
    export_flow_conversion, export_confident_final, export_model_final = gen_flow_comparison_result(final_all_order)
    
    export_flow_conversion.to_csv(f'{target_dir}/export_main_flow_conversion.csv', index=False)
    export_confident_final.to_csv(f'{target_dir}/export_flow_confidence_level.csv', index=False)
    export_model_final.to_csv(f'{target_dir}/export_model.csv', index=False)
