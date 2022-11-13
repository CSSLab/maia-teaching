import numpy as np
import pandas as pd
import chess
import json
import os


# output_filename = 'sesse_parsed.pkl'


# check that every move in a position is searched up to atleast depth <threshold>
def depth_checker(data, threshold):
    for mv in data['refutation_lines']:
        if int(data['refutation_lines'][mv]['depth']) < threshold:
            return False
    return True

nodes = []
fens = []
history = []
move_ply = []
score = []
depth = []
invalid = []

directory = os.fsencode('/data/sesse')

for file in os.listdir(directory):
    filename = os.fsdecode(file)
    with open('/data/sesse/' + str(filename)) as f:
        try:
            data = json.load(f)
            if data['depth'] is not None and 24 < int(data['depth']) and depth_checker(data, 24) and data['refutation_lines'] != {}:
                
                score_resultant = {}
                depth_resultant = []
                invalid_bool = False
                for key in data['refutation_lines']:
                    printed_score = int(data['refutation_lines'][key]['score'][1])
                    if data['refutation_lines'][key]['score'][0] == 'cp':
                        score_resultant[key] = printed_score
                    else:
                        # from white's perspective
                        pv_len = len(data['refutation_lines'][key]['pv'])
                        last_move = data['refutation_lines'][key]['pv'][pv_len - 1]
                        if last_move[len(last_move) - 1] != '#':
                            # invalid data point
                            invalid_bool = True
                                            
                        if pv_len % 2 == 0:
                            sign = -1
                        else:
                            sign = 1
                            
                        if chess.Board(data['position']['fen']).turn:
                            color = 1
                        else:
                            color = -1  
                        
                            
                        score_resultant[key] = sign * color * 10000
                    depth_resultant.append(data['refutation_lines'][key]['depth'])
                
                hist = data['position']['history']
                
                if len(list(chess.Board(data['position']['fen']).legal_moves)) != len(score_resultant):
                    invalid_bool = True
                
                nodes.append(data['nodes'])
                fens.append(data['position']['fen'])
                history.append(hist)
                move_ply.append(len(hist))
                score.append(score_resultant)
                depth.append(depth_resultant)
                invalid.append(invalid_bool)
        
        except KeyError:
            print("inconsistent data point. ignore it")
        except ValueError:
            print('help')

df = pd.DataFrame(np.array([fens, move_ply, history, score, nodes, depth, invalid]).T, columns = ['fen', 'move_num', 'history', 'sf_eval', 'nodes', 'depths', 'invalid'])

df = df.drop(df[invalid].index)
df = df.reset_index(drop=True)
df = df.drop(columns='invalid')

df.to_pickle(output_filename)