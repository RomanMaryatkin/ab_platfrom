import pandas as pd
import pandahouse
from statsmodels.stats.proportion import proportions_ztest
from statsmodels.stats.multitest import multipletests
from scipy import stats
import clickhouse_connect
import json

class Experiment:
    def __init__(self, exp_os, exp_toggle, exp_name, start_date, end_date, test_params):
        self.exp_os = exp_os
        self.exp_toggle = exp_toggle
        self.exp_name = exp_name
        self.start_date = start_date
        self.end_date = end_date
        self.test_params = test_params

    def display(self):
        print("Experiment OS:", self.exp_os)
        print("Experiment Toggle:", self.exp_toggle)
        print("Experiment Name:", self.exp_name)
        print("Start Date:", self.start_date)
        print("End Date:", self.end_date)
        print("Test Parameters:", self.test_params)

def __get_column_lists(df):
    columns_list = df.columns.tolist()
    list_num = [string for string in columns_list if string.endswith('_num')]
    list_den = [string for string in columns_list if string.endswith('_den')]
    list_mean = [string for string in columns_list if string.endswith('_mean')]
    return list_num, list_den, list_mean

def get_data(experiment, connection, query=None):
    if query is None:
        query = f'''
            select 
            	coalesce(t1.user_pseudo_id, '') as user_pseudo_id,
                '{experiment.exp_os}' as os,
                coalesce(max(arrayElement(arrayFilter(x -> x.1 == '{experiment.exp_toggle}', ab_tests).2, 1)),'') as group_field,
                max(any_active > 0)                              as any_active_flg,
                --main product metrics
                max(complete_purchase > 0)                       as complete_purchase_num,
                any_active_flg                                   as complete_purchase_den,
                max(payed_purchase > 0)                          as payed_purchase_num,
                any_active_flg                                   as payed_purchase_den,
                max(cancels > 0)                                 as cancels_num,
                any_active_flg                                   as cancels_den,
                toFloat64(max(add_to_cart > 0))                  as add_to_cart_num,
                any_active_flg                                   as add_to_cart_den,
                toFloat64(max(add_to_cart_catalog > 0))          as add_to_cart_catalog_num,
                any_active_flg                                   as add_to_cart_catalog_den,
                toFloat64(max(add_to_cart_search > 0))           as add_to_cart_search_num,
                any_active_flg                                   as add_to_cart_search_den,
                toFloat64(max(add_to_cart_product_page > 0))     as add_to_cart_product_page_num,
                any_active_flg                                   as add_to_cart_product_page_den,
                toFloat64(max(search_for_item > 0))              as search_for_item_num,
                any_active_flg                                   as search_for_item_den,
            	toFloat64(max(view_catalog_all > 0))             as view_catalog_all_num,
                any_active_flg                                   as view_catalog_all_den,
            	toFloat64(max(view_catalog_1 > 0))               as view_catalog_1_num,
                any_active_flg                                   as view_catalog_1_den,
            	toFloat64(max(view_catalog_2 > 0))               as view_catalog_2_num,
                any_active_flg                                   as view_catalog_2_den,
            	toFloat64(max(view_catalog_3 > 0))               as view_catalog_3_num,
                any_active_flg                                   as view_catalog_3_den,
            	toFloat64(max(view_product_page > 0))            as view_product_page_num,
                any_active_flg                                   as view_product_page_den,
            	toFloat64(max(view_product_page_search > 0))     as view_product_page_search_num,
                any_active_flg                                   as view_product_page_search_den,
            	toFloat64(max(view_product_page_catalog > 0))    as view_product_page_catalog_num,
                any_active_flg                                   as view_product_page_catalog_den,
                --main fin metrics
                toFloat64(sum(revenue))                            as arpu_num,
                any_active_flg                                     as arpu_den,
                toFloat64(sumIf(revenue, complete_purchase > 0))   as arppu_num,
                max(complete_purchase > 0)                         as arppu_den,
                toFloat64(sum(margin))                             as margin_per_user_num,
                any_active_flg                                     as margin_per_user_den,
                toFloat64(sum(revenue))                            as avg_check_num,
                toFloat64(sum(complete_purchase))                  as avg_check_den
            from dwh_datamart.daily_aud_aggregate_app t1
            where 1
            	and date(t1.event_date) >= '{experiment.start_date}' and date(t1.event_date) <= '{experiment.end_date}'
            	and t1.platform_detailed = '{experiment.exp_os}'
            	and t1.brand_name = 'LO'
            	and arrayElement(arrayFilter(x -> x.1 == '{experiment.exp_toggle}', t1.ab_tests).1, 1) = '{experiment.exp_toggle}'
            	and t1.any_active = 1
            group by 1,2
            '''
    try:
        df = pandahouse.read_clickhouse(query, connection=connection)
        list_num, list_den, _ = __get_column_lists(df)
        for i in range(len(list_num)):
            num = df[list_num[i]]
            den = df[list_den[i]]
            df[f'{list_num[i]}_tailor_mean'] = num.mean() / den.mean() + 1 / den.mean() * (num - num.mean() / den.mean() * den)
        df['bakets'] = df['user_pseudo_id'].apply(lambda x: hash(x + f'{experiment.exp_toggle}') % 200)
        df.groupby(['group_field', 'bakets'])['user_pseudo_id'].nunique().reset_index()
        return df
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

def get_results(df):
    control = df[df['group_field'] == '0']
    test = df[df['group_field'] > '0']
    group_list=sorted(test['group_field'].unique())

    list_num, list_den, list_mean = __get_column_lists(df)

    comparison_group_list = []
    metric_name_list = []
    metric_type_list = []
    control_sample_size = []
    test_sample_size = []
    control_meric_list = []
    test_metric_list = []
    p_value_list = []
    significance_list = []
    
    for gr in group_list:
        for i in range(0,len(list_num),1):
            control_num = control.groupby('group_field').sum(numeric_only=True).reset_index()[list_num[i]]
            control_den = control.groupby('group_field').sum(numeric_only=True).reset_index()[list_den[i]]
            test_num = test[test['group_field'] == gr].groupby('group_field').sum(numeric_only=True).reset_index()[list_num[i]]
            test_den = test[test['group_field'] == gr].groupby('group_field').sum(numeric_only=True).reset_index()[list_den[i]]
            
            if (control_num[0] < control_den[0]) & (test_num[0] < test_den[0]):
                _, p_value = proportions_ztest([control_num, test_num], 
                                                nobs=[control_den, test_den])
                
                p_value_list.append(p_value[0])
            else:
                p_value_list.append(None)
            
            comparison_group_list.append(str(0) + ' vs ' +  gr)
            metric_name_list.append(list_num[i].replace('_num', ''))
            metric_type_list.append('proportion')
            control_sample_size.append(len(control.index))
            test_sample_size.append(len(test[test['group_field'] == gr].index))
            control_meric_list.append(round(control_num[0]/control_den[0], 4))
            test_metric_list.append(round(test_num[0]/test_den[0], 4))

        for i in range(0,len(list_mean),1):    
            control_bakets = control.groupby('bakets').mean(numeric_only=True)[list_mean[i]]
            test_bakets = test[test['group_field'] == gr].groupby('bakets').mean(numeric_only=True)[list_mean[i]]
            
            _, p_value = stats.ttest_ind(control_bakets, test_bakets)

            comparison_group_list.append(str(0) + ' vs ' +  gr)
            metric_name_list.append(list_mean[i].replace('_num_tailor_mean', ''))
            metric_type_list.append('mean')
            control_sample_size.append(len(control.index))
            test_sample_size.append(len(test[test['group_field'] == gr].index))
            control_meric_list.append(round(control_bakets.mean(numeric_only=True), 4))
            test_metric_list.append(round(test_bakets.mean(numeric_only=True), 4))
            p_value_list.append(p_value)

    res_df = pd.DataFrame({
        'groups': comparison_group_list,
        'metric_name': metric_name_list,
        'metric_type': metric_type_list,
        'control_sample_size': control_sample_size,
        'test_sample_size': test_sample_size,
        'control': control_meric_list,
        'test': test_metric_list,
        'p_value': p_value_list
    })

    res_pivot_df = pd.merge(
        res_df[res_df['metric_type'] == 'mean'][['groups', 'metric_name', 
                                                 'control_sample_size', 'test_sample_size',
                                                 'control', 'test', 
                                                 'p_value']]
        .rename(
            columns={'control_sample_size': 'control_sample_size_mean',
                     'test_sample_size': 'test_sample_size_mean',
                     'control': 'control_mean',
                     'test': 'test_mean',
                     'p_value': 'p_value_ttest'
                     }),
        res_df[res_df['metric_type'] == 'proportion']
        .rename(
            columns={'control_sample_size': 'control_sample_size_proportion',
                     'test_sample_size': 'test_sample_size_proportion',
                     'control': 'control_proportion',
                     'test': 'test_proportion',
                     'p_value': 'p_value_ztest'
                     }), 
        how='inner',
        on=['groups', 'metric_name']
    )

    res_pivot_df['control_sample_size'] = res_pivot_df.control_sample_size_proportion.combine_first(res_pivot_df.control_sample_size_mean)
    res_pivot_df['test_sample_size'] = res_pivot_df.test_sample_size_proportion.combine_first(res_pivot_df.test_sample_size_mean)
    res_pivot_df['control'] = res_pivot_df.control_proportion.combine_first(res_pivot_df.control_mean)
    res_pivot_df['test'] = res_pivot_df.test_proportion.combine_first(res_pivot_df.test_mean)

    p_values_ttest_correction_list = res_pivot_df.p_value_ttest
    ttest_reject_list, p_values_ttest_corrected_list, _, _ = multipletests(p_values_ttest_correction_list, method='holm')
    res_pivot_df['p_value_ttest_corrected'] = p_values_ttest_corrected_list
    res_pivot_df['significance_ttest'] = ttest_reject_list
    res_pivot_df.significance_ttest = res_pivot_df.significance_ttest.astype(int)

    p_values_ztest_correction_list = res_pivot_df.p_value_ztest
    ztest_reject_list, p_values_ztest_corrected_list, _, _ = multipletests(p_values_ztest_correction_list, method='holm')
    res_pivot_df['p_value_ztest_corrected'] = p_values_ztest_corrected_list
    res_pivot_df['significance_ztest'] = ztest_reject_list
    res_pivot_df.significance_ztest = res_pivot_df.significance_ztest.astype(int)

    res_pivot_df = res_pivot_df[['groups', 'metric_name',
                                 'control_sample_size', 'test_sample_size',
                                 'control', 'test',
                                 'p_value_ttest', 'p_value_ttest_corrected', 'significance_ttest',
                                 'p_value_ztest', 'p_value_ztest_corrected', 'significance_ztest'
                                 ]]
    
    return res_df, res_pivot_df

def get_publish_results(experiment, df):
    df['diff_abs'] = df['test'] - df['control']
    df['diff_rel'] = ((df['test'] - df['control']) / df['control']) * 100

    df['period'] = f'{experiment.start_date} - {experiment.end_date}'
    df['toggle'] = experiment.exp_name
    df['params'] = experiment.test_params
    df['os'] = experiment.exp_os

    if 'significance' in set(df.columns.tolist()):
        df['significance_text'] = df['significance'].apply(
            lambda x: 'Есть стат. значимая разница' if x == 1 else 'Нет стат. значимой разницы'
        )
    elif 'significance_ttest' in set(df.columns.tolist()):
        df['significance_text'] = df['significance_ttest'].apply(
            lambda x: 'Есть стат. значимая разница' if x == 1 else 'Нет стат. значимой разницы'
        )

    df = df[[
        'period', 'os', 'toggle', 'params',
        'groups', 'metric_name',
        'control_sample_size', 'test_sample_size',
        'test', 'control',
        'diff_abs', 'diff_rel',
        'p_value_ztest_corrected', 'significance_ztest',
        'p_value_ttest_corrected', 'significance_ttest',
        'significance_text'
             ]]

    return df

def save_results_to_excel(experiment, df):
    df.to_excel(
    f'{experiment.exp_os.lower()}_{experiment.start_date}-{experiment.end_date}_param_{experiment.test_params}.xlsx',
    index=False
    )

def save_results_to_db(experiment, conn_dict, metrics_final_conf_df):
    client = clickhouse_connect.get_client(
        host=conn_dict['url'], 
        port=conn_dict["port"],
        username=conn_dict["user"],
        password=conn_dict["password"]
    )

    #CREATE TABLE sandbox.analysis_results on cluster analitica_cluster
    #(
    #    period String,
    #    os String,
    #    toggle String,
    #    params String,
    #    groups String,
    #    metric_name String,
    #    control_sample_size Int64, 
    #	 test_sample_size Int64,     
    #    test Float64,
    #    control Float64,
    #    diff_abs Float64,
    #    diff_rel Float64,
    #    p_value_ztest Float64,
    #    significance_ztest UInt8,
    #    p_value_ttest Float64,
    #    significance_ttest UInt8,
    #    significance_text String
    #)
    #engine = ReplicatedMergeTree('/clickhouse/tables/{database}/{table}/{uuid}', '{replica}')
    #order by period
    #SETTINGS index_granularity = 0000

    client.query(f'''delete from sandbox.analysis_results
                     where toggle = '{experiment.exp_name}' 
                     and params = '{experiment.test_params}' 
                     and os = '{experiment.exp_os}' ''')

    # Insert data into ClickHouse table
    query = 'insert into sandbox.analysis_results values'
    for index, row in metrics_final_conf_df.iterrows():
        values = tuple(row)
        query += f'\n{values},'

    query = query[:-1]

    client.query(query) 