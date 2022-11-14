import io
import multiprocessing
import os.path
import requests
import pickle
import sys

import chess
import pandas as pd
import chess.engine
import maia_lib

# This piece of code generates data for the budget problem
#          We run stockfish according to different budget 
#          allocation strategies, and save the results in
#          a pandas data frame

def get_cp_num(cp):
    try:
        return cp.relative.cp
    except AttributeError:
        return 10000 if cp.relative.mate() > 0 else -10000

_sf_default_path = '/home/sepehr/.conda/envs/myenv/lib/python3.8/site-packages/maia_lib/model_utils/stockfish/stockfish_14_x64_bmi2'

# takes in board and budget
# returns dictionary of (move, cp score) pairs according to the budget allocation strategy 
def eval_board(board, maia_eval, budget, type_run) -> dict:
    
    return_dict = {}
    
    if isinstance(board, str):
        board = chess.Board(board)
    elif isinstance(board, maia_lib.LeelaBoard):
        board = board.pc_board

    uniform_factor = 1. / float(len(list(board.legal_moves)))
    
    for mv in board.legal_moves:
        board.push(mv)
        
        if type_run == 'maia_weighted':
            node_weight = maia_eval[str(mv)]
        if type_run == 'uniform':
            node_weight = uniform_factor

        engine = chess.engine.SimpleEngine.popen_uci(_sf_default_path)
        engine.configure({"Threads": max(1, multiprocessing.cpu_count() - 16)})
        analysis = engine.analyse(
            board,
            chess.engine.Limit(nodes=(node_weight * budget)),
        )
        engine.quit()
        
        # score, nodes
        return_dict[mv.uci()] = (-1 * get_cp_num(analysis["score"]), analysis["nodes"])
        
        board.pop()

    return return_dict

def eval_boards(boards, maia_evals, budget, type_run):
    ret_list = []
    for i in range(len(boards)):
        print(i)
        b = boards[i]
        m = maia_evals[i]
        
        ret_list.append(eval_board(b, m, budget, type_run))
    return ret_list


num_nodes = 10000000
type_run = 'maia_weighted'
pv_percentage = 0.5

print(f'budget : ' + str(num_nodes))
print(f'type : ' + type_run)
print(f'pv percentage : ' + str(pv_percentage))

input_df = 'sesse_with_maia_small.pkl'
output_file = f'sesse_small_sf_{type_run}_with_pv_{pv_percentage}.pkl'
df = pd.read_pickle(input_df)

######## initial pv step #########
if 0 < pv_percentage:
    best_mvs = []
    for i in range(len(df)):

        board = chess.Board(df.iloc[i]['fen'])

        engine = chess.engine.SimpleEngine.popen_uci(_sf_default_path)
        engine.configure({"Threads": max(1, multiprocessing.cpu_count() - 16)})
        analysis = engine.analyse(
            board,
            chess.engine.Limit(nodes=(pv_percentage * num_nodes)),
        )
        engine.quit()

        best_mvs.append((str(analysis['pv'][0]), get_cp_num(analysis["score"])))

    df['pre_runs'] = best_mvs

    print('done preruns')
##################################

counter = 0

evals = [0] * len(df)
for i in range(len(df)):
    evals[i] = eval_board(chess.Board(df.iloc[i]['fen']), df.iloc[i]['maia_9'], (1. - pv_percentage) * num_nodes, type_run)
    print(counter)
    counter += 1
    if i % 25 == 0:
        df[f'sf_{type_run}'] = evals
        
        # save
        with open(output_file, 'wb') as f:
            pickle.dump(df, f)

df[f'sf_{type_run}'] = evals
with open(output_file, 'wb') as f:
    pickle.dump(df, f)