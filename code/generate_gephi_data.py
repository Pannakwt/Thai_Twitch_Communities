import json
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)


def preprocessing(file):
    """
    Import data from file and change data type to reduce comparison time.
    """

    logging.info('Importing data')
    with open(file) as read_file:
        data = json.load(read_file)

    logging.info('Converting dict of lists into dict of sets')
    for key in data:
        data[key] = set(data[key])
    
    return data


def create_gephi_data(data, overlap_threshold):
    """
    Creating node and edge data for using in gephi.

    The overlap_threshold is minimum overlap value between two keys that can be an edge.

    Data is return as a list of each node (node_data) and list of each edge (edge_data)
    """

    completed_key = []
    node_data = []
    edge_data = []

    logging.info('Creating node and edge data, minimum edge value is %d', overlap_threshold)
    for count, source in enumerate(data, start=1):

        logging.info('Creating data from key: %d/%d', count, len(data))
        for target in data:
            if ((target != source) and (target not in completed_key)):
                overlap = data[source] & data[target]
                
                # Create edge data.
                if len(overlap) > overlap_threshold:
                    edge = [source, target, len(overlap)]
                    edge_data.append(edge)

            # Create node data.
            elif (target == source):
                node = [source, target, len(data[source])]
                node_data.append(node)
                
        # Use completed_key to avoid adding the same edge e.g. a-b and b-a.
        completed_key.append(source)
    logging.info('The data has been created')

    return node_data, edge_data


def export_data(node_filename, edge_filename, node_data, edge_data):
    """
    Transform data in correct format for using in gephi and export as a csv file.
    """

    logging.info('Defining column name and creating dataframe of node and edge data')
    node_colname = ['Id', 'Label', 'Count']
    edge_colname = ['Source', 'Target', 'Weight']

    node_df = pd.DataFrame(node_data, columns=node_colname)
    edge_df = pd.DataFrame(edge_data, columns=edge_colname)

    logging.info('Exporting data')
    node_df.to_csv(f'../data/{node_filename}.csv', index=False)
    edge_df.to_csv(f'../data/{edge_filename}.csv', index=False)
    logging.info('Exported')


def main():
    data = preprocessing('../data/twitch_data.json')
    node_data, edge_data = create_gephi_data(data, overlap_threshold=0)
    export_data('node_data', 'edge_data', node_data, edge_data)


if __name__ == '__main__':
    main()