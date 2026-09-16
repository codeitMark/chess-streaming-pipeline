import io
import json
import os
import shutil
import chess
import chess.engine
import chess.pgn

# Stockfish binary from STOCKFISH_PATH, the system PATH, or the Debian default location
STOCKFISH_PATH = (
    os.environ.get("STOCKFISH_PATH")
    or shutil.which("stockfish")
    or "/usr/games/stockfish"
)

# Default search depth and the centipawn thresholds used to classify moves
DEFAULT_DEPTH = 17
MOVE_RANKS = ("Brilliant", "Good", "Bad", "Blunder")
GOOD_MAX_LOSS = 100
BLUNDER_MIN_LOSS = 400
BRILLIANT_MAX_LOSS = 30
BRILLIANT_MIN_SACRIFICE = 500

# Sample chess.com movetext, used when the module is run directly
movetext ="1. e4 {[%clk 0:09:57.9]} 1... e5 {[%clk 0:09:58.9]} 2. Nc3 {[%clk 0:09:56.4]} 2... Nf6 {[%clk 0:09:56.7]} 3. Nf3 {[%clk 0:09:52.6]} 3... Nc6 {[%clk 0:09:56.1]} 4. Bc4 {[%clk 0:09:25.9]} 4... Bb4 {[%clk 0:09:53.6]} 5. a3 {[%clk 0:09:21.2]} 5... Bxc3 {[%clk 0:09:52]} 6. dxc3 {[%clk 0:09:15.2]} 6... Nxe4 {[%clk 0:09:50.7]} 7. Be3 {[%clk 0:08:46.6]} 7... O-O {[%clk 0:09:42.6]} 8. Qd5 {[%clk 0:08:36.3]} 8... Qf6 {[%clk 0:09:28.2]} 9. Qxe4 {[%clk 0:08:25.9]} 9... d5 {[%clk 0:09:21.8]} 10. Qxd5 {[%clk 0:08:17.1]} 10... Be6 {[%clk 0:09:16.8]} 11. Qe4 {[%clk 0:07:56.6]} 11... Bf5 {[%clk 0:09:07.1]} 12. Qd5 {[%clk 0:07:34.8]} 12... Rad8 {[%clk 0:09:05.4]} 13. Qb5 {[%clk 0:06:52.2]} 13... Rb8 {[%clk 0:08:48.7]} 14. O-O-O {[%clk 0:06:33.2]} 14... e4 {[%clk 0:08:37.6]} 15. Nd4 {[%clk 0:06:12]} 15... Bg4 {[%clk 0:08:28.4]} 16. f3 {[%clk 0:05:36.7]} 16... exf3 {[%clk 0:08:22.6]} 17. gxf3 {[%clk 0:05:36]} 17... Bd7 {[%clk 0:08:10.5]} 18. Qc5 {[%clk 0:04:41.8]} 18... Rfe8 {[%clk 0:07:57.3]} 19. Rhg1 {[%clk 0:04:32.3]} 19... Rxe3 {[%clk 0:07:53.1]} 20. Nxc6 {[%clk 0:03:08.1]} 20... Qf4 {[%clk 0:07:43.9]} 21. Nxb8 {[%clk 0:02:51.9]} 21... Re5+ {[%clk 0:07:41.5]} 22. Kb1 {[%clk 0:01:09.6]} 22... Rxc5 {[%clk 0:07:33.4]} 23. Rge1 {[%clk 0:01:02.8]} 23... Re5 {[%clk 0:07:18.7]} 24. Rxe5 {[%clk 0:00:35.8]} 24... Qxe5 {[%clk 0:07:16.8]} 25. Nxd7 {[%clk 0:00:32.2]} 25... Qe7 {[%clk 0:07:12.9]} 26. Nc5 {[%clk 0:00:09.8]} 26... g5 {[%clk 0:07:09]} 27. Ne4 {[%clk 0:00:07.6]} 27... Qe8 {[%clk 0:07:00.1]} 28. Nf6+ {[%clk 0:00:06.4]} 28... Kg7 {[%clk 0:06:57.8]} 29. Nxe8+ {[%clk 0:00:05.7]} 29... Kh6 {[%clk 0:06:56]} 30. Rd5 {[%clk 0:00:04.6]} 30... Kh5 {[%clk 0:06:54.7]} 31. Nf6+ {[%clk 0:00:02.1]} 31... Kh4 {[%clk 0:06:54.6]} 32. Nxh7 {[%clk 0:00:00.8]} 32... Kh5 {[%clk 0:06:53.5]} 33. Nxg5 {[%clk 0:00:00.7]} 33... Kg6 {[%clk 0:06:50.9]} 34. Nxf7 {[%clk 0:00:00.6]} 34... Kxf7 {[%clk 0:06:49.8]}"

# Material balance in centipawns from one player's perspective
def _material_balance(board: chess.Board, color: chess.Color) -> int:
    values = {
        chess.PAWN: 100,
        chess.KNIGHT: 320,
        chess.BISHOP: 330,
        chess.ROOK: 500,
        chess.QUEEN: 900,
    }
    own = sum(
        len(board.pieces(piece_type, color)) * value
        for piece_type, value in values.items()
    )
    opponent = sum(
        len(board.pieces(piece_type, not color)) * value
        for piece_type, value in values.items()
    )
    return own - opponent


# Stockfish score in centipawns from the moving side's view
def _evaluation(score: chess.engine.PovScore, color: chess.Color) -> int:
    return score.pov(color).score(mate_score=100_000) or 0


# Classifies a move by centipawn loss
def _rank_move(
    best_loss: int,
    material_loss: int,
) -> str:
    if (material_loss >= BRILLIANT_MIN_SACRIFICE
            and best_loss <= BRILLIANT_MAX_LOSS):
        return "Brilliant"
    if best_loss <= GOOD_MAX_LOSS:
        return "Good"
    if best_loss < BLUNDER_MIN_LOSS:
        return "Bad"
    return "Blunder"


# Ranks every ply of a game, writes the result to output_path and returns it
def analyze_chess_game(
    game_text: str = movetext,
    output_path: str = "move_ranks.json",
    depth: int = DEFAULT_DEPTH,
) -> dict[str, dict[str, str]]:
    game = chess.pgn.read_game(io.StringIO(game_text))
    if game is None:
        raise ValueError("Variable game_text does not contain a valid game moves")

    board = game.board()
    move_ranks: dict[str, dict[str, str]] = {}

    # Compare the engine's evaluation before and after each move, and material after its best reply
    with chess.engine.SimpleEngine.popen_uci(STOCKFISH_PATH) as engine:
        for ply_number, move in enumerate(game.mainline_moves(), start=1):
            color = board.turn
            before_material = _material_balance(board, color)
            before = engine.analyse(board, chess.engine.Limit(depth=depth))
            best_score = _evaluation(before["score"], color)

            board.push(move)
            after = engine.analyse(board, chess.engine.Limit(depth=depth))
            played_score = _evaluation(after["score"], color)

            move_loss = max(0, best_score - played_score)
            material_loss = 0
            opponent_reply = after.get("pv", [None])[0]
            if opponent_reply is not None and not board.is_game_over():
                board.push(opponent_reply)
                material_loss = max(
                    0,
                    before_material - _material_balance(board, color),
                )
                board.pop()

            move_ranks[str(ply_number)] = {
                "side": "White" if color == chess.WHITE else "Black",
                "rank": _rank_move(
                    move_loss,
                    material_loss,
                ),
            }

    # Save the ranks to output_path
    with open(output_path, "w", encoding="utf-8") as output_file:
        json.dump(move_ranks, output_file, indent=2)
        output_file.write("\n")

    return move_ranks

# Analyses the sample game when the module is run directly
if __name__ == "__main__":
    print("Starting Stockfish analysis inside Docker...\n")
    analysis_data = analyze_chess_game()

    print("--- MOVE CLASSIFICATIONS OUTPUT ---")
    print(json.dumps(analysis_data, indent=2, default=str))