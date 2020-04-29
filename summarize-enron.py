import sys
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from typing import List


def parse_pipe_separated_col(recipients: str) -> List[str]:
    """
    A function that transforms pipe separated recipients to recipient list
    Passed as a function parameter in `pd.Series.apply`
    """
    return recipients.strip().split('|')


def count_sent_received_on_person(data: pd.DataFrame, exploded_data: pd.DataFrame) -> pd.DataFrame:
    """
    This function output a csv file that is required by question (1)
    :param data: raw_data
    :param exploded_data: recipients_exploded_data
    :return:
    """
    group_sender = data.groupby('sender')['message identifier'].count().rename('sent')
    group_receiver = exploded_data.groupby('recipients')['message identifier'].count().rename('received')
    result = pd.concat([group_sender, group_receiver], axis=1) \
        .fillna(0).astype(np.int64).sort_values('sent')

    csv_path = os.getcwd() + '/person-sent-received.csv'
    result.to_csv(csv_path, index_label='person')
    print("csv file successfully saved, please check at {}".format(csv_path))
    return result


def plot_email_num_overtime(data: pd.Series, derived_data: pd.DataFrame) -> None:
    """
    This function plots a graph required by question (2)
    params:
        `data`: data frame `weekly_sent`;
        `derived_data`: data frame `person_sent_received`
    """
    # compute number of weeks during which the most prolific 100 individual has sent emails
    weeks = data.shape[0]
    sent = derived_data.iloc[-100:, 0]

    fig = plt.figure(figsize=(24, 16))

    ax1 = plt.subplot(212)
    ax1.bar(sent.index, sent / weeks, align='center')
    ax1.set_xticklabels(sent.index, rotation=90)
    ax1.set_title("Number of individual's weekly average sent", fontsize=20)
    ax1.set_xlabel('individuals', fontsize=15)
    ax1.set_ylabel('number of weekly average sent', fontsize=15)

    ax2 = plt.subplot(221)
    ax2.plot(data.index, data)
    ax2.set_xticks(data.index[::25])
    ax2.set_xticklabels(data.index[::25], rotation=45)
    ax2.set_xlabel('weeks (format of %Y-%U)', fontsize=15)
    ax2.set_ylabel('number of weekly sent', fontsize=15)
    ax2.set_title('changing of accumulative weekly sent over time (the most prolific 100 individuals)', fontsize=20)

    ax3 = plt.subplot(222)
    ax3.hist(sent / weeks, density=False, bins=30)
    ax3.set_xlabel('number of average weekly sent by one individual', fontsize=15)
    ax3.set_ylabel('frequency / count', fontsize=15)
    ax3.set_title('Distribution of average weekly sent (among the most prolific 100 individuals)', fontsize=20)

    save_path = os.getcwd() + "/Visualization (2).png"
    plt.savefig(save_path)
    print('figure successfully saved, please check at {}'.format(save_path))


def plot_unique_contacts(relative: pd.DataFrame, weekly_sent: pd.DataFrame,
                         unique_contacts: pd.DataFrame) -> None:
    """
    This function plots a graph required by question (3)
    """
    weekly = weekly_sent[unique_contacts.index].dropna()
    weeks = unique_contacts.shape[0]

    fig, axes = plt.subplots(1, 2, figsize=(24, 8))

    axes[0].plot(unique_contacts.index, unique_contacts['sender'], label='weekly unique contacts')
    axes[0].plot(weekly.index, weekly, label='weekly sent')
    axes[0].legend(loc='upper right')
    axes[0].set_xticks(unique_contacts.index[::10])
    axes[0].set_xticklabels(unique_contacts.index[::10], rotation=45)
    axes[0].set_title('changing of sending and unique contacts over time (among the most prolific 100 individuals)', fontsize=15)
    axes[0].set_xlabel('weeks (format of %Y-%U)', fontsize=15)
    axes[0].set_ylabel('number of emails sent/received', fontsize=15)

    axes[1].hist(relative['relative number'], density=False, bins=30)
    axes[1].set_title(
        'Distribution of relative number (ratio of unique contacts over number of sending) of the same person', fontsize=15)
    axes[1].set_xlabel('relative number', fontsize=15)
    axes[1].set_ylabel('frequency / count', fontsize=15)

    plt.subplots_adjust(wspace=0.1)
    save_path = os.getcwd() + '/Visualization (3).png'
    plt.savefig(save_path)
    print("figure successfully saved, please check at {}".format(save_path))


if __name__ == "__main__":
    plt.style.use('seaborn')
    file_name = sys.argv[1]
    raw_data = pd.read_csv(file_name,
                           names=['time', 'message identifier', 'sender',
                                  'recipients', 'topic', 'mode'],
                           index_col=0)
    raw_data['recipients'] = raw_data['recipients'].astype(str)
    # parse pipe-separated string to list and expand them as cloumn value
    raw_data['recipients'] = raw_data['recipients'].apply(parse_pipe_separated_col)
    recipients_exploded_data = raw_data.explode('recipients')

    person_sent_received = count_sent_received_on_person(raw_data,recipients_exploded_data)
    # prepare data for visualization (2)
    prolific = raw_data[raw_data.sender.isin(person_sent_received.index[-100:])]
    prolific.index = pd.to_datetime(prolific.index, unit='ms')
    prolific = prolific.reset_index()
    prolific['time'] = prolific['time'].dt.strftime('%Y-%U')
    weekly_sent = prolific.groupby('time')['message identifier'].count()
    plot_email_num_overtime(weekly_sent, person_sent_received)
    # prepare data for visualization (3)
    contacts = recipients_exploded_data[
        recipients_exploded_data.recipients.isin(person_sent_received.index[-100:])].reset_index()
    contacts['time'] = pd.to_datetime(contacts['time'], unit='ms').dt.strftime('%Y-%U')
    unique_contacts = pd.DataFrame(contacts.groupby('time')['sender'].nunique())
    unique_contacts['sent'] = weekly_sent[unique_contacts.index]
    unique_contacts = unique_contacts.dropna()
    relative = pd.DataFrame(contacts.groupby('recipients')['sender'].nunique())
    relative['sent'] = person_sent_received.loc[relative.index, 'sent']
    relative['relative number'] = relative['sender'] / relative['sent']
    plot_unique_contacts(relative, weekly_sent, unique_contacts)



