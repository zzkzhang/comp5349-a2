from pyspark.sql import SparkSession
from pyspark.sql.functions import explode


def sequence_answer(rdd):
    sequence_list = []
    context = rdd[0][0]
    title = rdd[1]
    qas = rdd[0][1]

    for i in range(int(len(context) / 2048) + 1):
        start_index = i * 2048
        if len(context) - start_index > 4096:
            end_index = start_index + 4096
        else:
            end_index = len(context)
        sequence = context[start_index:end_index]
        for qa in qas:
            if qa[2] is True:
                sequence_list.append([title, sequence, start_index, end_index, 'ins', 0, 0, qa[2], qa[3]])
            else:
                answers = qa[0]
                for answer in answers:
                    if len(answer) != 0:
                        answer_start_index = answer[0]
                        answer_end_index = answer_start_index + len(answer[1])
                        if (answer_start_index >= start_index and answer_start_index <= end_index) or (
                                answer_end_index >= start_index and answer_end_index <= end_index):
                            overlap = 'Yes'
                        else:
                            overlap = 'No'
                        if overlap == 'Yes':
                            sequence_list.append(
                                [title, sequence, start_index, end_index, 'ps', answer_start_index, answer_end_index,
                                 qa[2], qa[3]])
                        else:
                            sequence_list.append(
                                [title, sequence, start_index, end_index, 'pns', answer_start_index, answer_end_index,
                                 qa[2], qa[3]])
                    else:
                        answer_start_index = 0
                        answer_end_index = 0
                        if (answer_start_index >= start_index and answer_start_index <= end_index) or (
                                answer_end_index >= start_index and answer_end_index <= end_index):
                            overlap = 'Yes'
                        else:
                            overlap = 'No'
                        if overlap == 'Yes':
                            sequence_list.append(
                                [title, sequence, start_index, end_index, 'ps', answer_start_index, answer_end_index,
                                 qa[2], qa[3]])
                        else:
                            sequence_list.append(
                                [title, sequence, start_index, end_index, 'pns', answer_start_index, answer_end_index,
                                 qa[2], qa[3]])
    return sequence_list


def unique_sequence(rdd):
    result_list = []
    if rdd[3] is not None:
        result_list.append([rdd[0], rdd[1], rdd[2], rdd[3], rdd[4], rdd[5]])
    elif rdd[3] is None and rdd[6] is not None:
        result_list.append([rdd[0], rdd[1], rdd[2], rdd[6], rdd[7], rdd[8]])
    elif rdd[3] is None and rdd[6] is None and rdd[9] is not None:
        result_list.append([rdd[0], rdd[1], rdd[2], rdd[9], rdd[10], rdd[11]])
    return result_list


def filter_ins_pns(rdd):
    filter_list = []
    ins_count = 0
    pns_count = 0
    for i in rdd[1]:
        ps_sum = 0
        other_title_count = 0
        for ps in i[0]:
            if ps[0] != rdd[0]:
                ps_sum += ps[1]
                other_title_count += 1
            else:
                ps_total_only = ps[1]
                pns_limit = int(ps[1])
        if other_title_count != 0:
            ins_limit = int(ps_sum / other_title_count)
        else:
            pns_limit = int(ps_total_only)

    if i[5] == 'ins':
        if ins_count <= ins_limit:
            ins_count += 1
            filter = [rdd[0], i[1], i[2], i[3], i[4], i[5]]
        else:
            pass
    elif i[5] == 'pns':
        if pns_count <= pns_limit:
            pns_count += 1
            filter = [rdd[0], i[1], i[2], i[3], i[4], i[5]]
        else:
            pass
    else:
        filter = [rdd[0], i[1], i[2], i[3], i[4], i[5]]
    filter_list.append(filter)
    return filter_list


def main():
    spark = SparkSession \
        .builder \
        .appName("COMP5349 A2") \
        .getOrCreate()
    test_data = "s3://comp5349-2022/test.json"
    test_init_df = spark.read.json(test_data)
    # train_data = "/content/drive/MyDrive/COMP5349-A2/train_separate_questions.json"
    # train_init_df = spark.read.json(train_data)
    test_data_df = test_init_df.select((explode("data").alias('data')))
    test_data_df = test_data_df.select(['data.paragraphs', 'data.title'])
    test_paragraph_df = test_data_df.withColumn('paragraphs', explode('paragraphs'))
    test_sequence_answer_rdd = test_paragraph_df.rdd.flatMap(sequence_answer)
    df_columns = ['title', 'source', 'question', 'sample', 'answer_start', 'answer_end']
    df_ps = test_sequence_answer_rdd.filter(lambda x: x[4] == 'ps').map(
        lambda x: (x[0], x[1], x[-1], x[4], x[5], x[6])).toDF(df_columns)
    df_pns = test_sequence_answer_rdd.filter(lambda x: x[4] == 'pns').map(
        lambda x: (x[0], x[1], x[-1], x[4], x[5], x[6])).toDF(df_columns)
    df_ins = test_sequence_answer_rdd.filter(lambda x: x[4] == 'ins').map(
        lambda x: (x[0], x[1], x[-1], x[4], x[5], x[6])).toDF(df_columns)
    df_join = df_ps.join(df_pns, ['title', 'source', 'question'], 'full').join(df_ins, ['title', 'source', 'question'],
                                                                               'full')
    process_rdd = df_join.rdd.flatMap(unique_sequence)
    ps_count = process_rdd.filter(lambda x: x[3] == 'ps').map(lambda x: ((x[0], x[2]), 1)).reduceByKey(
        lambda x, y: x + y).map(lambda x: (x[0][1], (x[0][0], x[1]))).groupByKey()
    ps_count_join_process = process_rdd.map(lambda x: (x[2], (x[0], x[1], x[3], x[4], x[5]))).join(ps_count).map(
        lambda x: (x[0], x[1][0], x[1][1])).map(
        lambda x: (x[1][0], (x[2], x[1][1], x[0], x[1][2], x[1][3], x[1][4]))).groupByKey()
    filter_ins_rdd = ps_count_join_process.flatMap(filter_ins_pns)
    result_rdd = filter_ins_rdd.map(lambda x: (x[1], x[2], x[4], x[5]))
    result_df = result_rdd.toDF()
    result_df.write.json('a2_output.json')


if __name__ == '__main__':
    main()
