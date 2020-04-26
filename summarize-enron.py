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
    recipients = recipients.strip()
    recipients_list = recipients.split('|')
    return recipients_list


def count_sender(data: pd.DataFrame) -> pd.DataFrame:
    """
    count number of sending from each individual
    """
    group_sender = data.groupby('sender')['message identifier'].count()
    group_sender = pd.DataFrame(group_sender)
    group_sender = group_sender.rename_axis('person')
    group_sender = group_sender.rename(columns={'message identifier': 'sent'})

    return group_sender


def count_receiver(data: pd.DataFrame) -> pd.DataFrame:
    """
    count number of email received of each individual
    """
    group_receiver = data.groupby('recipients')['message identifier'].count()
    group_receiver = pd.DataFrame(group_receiver)
    group_receiver = group_receiver.rename_axis('person')
    group_receiver = group_receiver.rename(columns={'message identifier': 'received'})

    return group_receiver


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
    ax1.set_title("Number of individual's weekly average sent")
    ax1.set_xlabel('individuals')
    ax1.set_ylabel('number of weekly average sent')

    ax2 = plt.subplot(221)
    ax2.plot(data.index, data)
    ax2.set_xticks(data.index[::25])
    ax2.set_xticklabels(data.index[::25], rotation=45)
    ax2.set_xlabel('weeks (format of %Y-%U)')
    ax2.set_ylabel('number of weekly sent')
    ax2.set_title('changing of accumulative weekly sent over time (the most prolific 100 individuals)')

    ax3 = plt.subplot(222)
    ax3.hist(sent / weeks, density=False, bins=30)
    ax3.set_xlabel('number of average weekly sent by one individual')
    ax3.set_ylabel('frequency / count')
    ax3.set_title('Distribution of average weekly sent (among the most prolific 100 individuals)')

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
    axes[0].set_title('changing of sending and unique contacts over time (among the most prolific 100 individuals)')
    axes[0].set_xlabel('weeks (format of %Y-%U)')
    axes[0].set_ylabel('number of emails sent/received')

    axes[1].hist(relative['relative number'], density=False, bins=30)
    axes[1].set_title(
        'Distribution of relative number (ratio of unique contacts over number of sending) of the same person')
    axes[1].set_xlabel('relative number')
    axes[1].set_ylabel('frequency / count')

    plt.subplots_adjust(wspace=0.1)
    save_path = os.getcwd() + '/Visualization (3).png'
    plt.savefig(save_path)
    print("figure successfully saved, please check at {}".format(save_path))


if __name__ == "__main__":
    file_name = sys.argv[1]
    raw_data = pd.read_csv(file_name,
                           names=['time', 'message identifier', 'sender',
                                  'recipients', 'topic', 'mode'],
                           index_col=0)
    raw_data['recipients'] = raw_data['recipients'].astype(str)

    group_sender = count_sender(raw_data)

    raw_data['recipients'] = raw_data['recipients'].apply(parse_pipe_separated_col)
    recipients_exploded_data = raw_data.explode('recipients')
    group_receiver = count_receiver(recipients_exploded_data)

    person_sent_received = group_sender.merge(group_receiver, how='outer', on='person')
    person_sent_received = person_sent_received.fillna(0).astype(np.int64).sort_values('sent')

    csv_path = os.getcwd() + '/person-sent-received.csv'
    person_sent_received.to_csv(csv_path)
    print("csv file successfully saved, please check at {}".format(csv_path))

    prolific = raw_data[raw_data.sender.isin(person_sent_received.index[-100:])]
    prolific.index = pd.to_datetime(prolific.index, unit='ms')
    prolific = prolific.reset_index()
    prolific['time'] = prolific['time'].dt.strftime('%Y-%U')
    weekly_sent = prolific.groupby('time')['message identifier'].count()
    plot_email_num_overtime(weekly_sent, person_sent_received)

    contacts = recipients_exploded_data[recipients_exploded_data.recipients.isin(person_sent_received.index[-100:])].reset_index()
    contacts['time'] = pd.to_datetime(contacts['time'], unit='ms').dt.strftime('%Y-%U')
    unique_contacts = pd.DataFrame(contacts.groupby('time')['sender'].nunique())
    unique_contacts['sent'] = weekly_sent[unique_contacts.index]
    unique_contacts = unique_contacts.dropna()
    relative = pd.DataFrame(contacts.groupby('recipients')['sender'].nunique())
    relative['sent'] = person_sent_received.loc[relative.index, 'sent']
    relative['relative number'] = relative['sender'] / relative['sent']
    plot_unique_contacts(relative, weekly_sent, unique_contacts)