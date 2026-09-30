# Main coordinator bot — always running (1 web dyno)
web: python3 main.py

# NOTE: worker.py is NOT listed here.
# It is spawned on-demand as Heroku one-off dynos by heroku_manager.py.
# Each paid user gets their own isolated one-off dyno when they start /clone.
#
# To run a worker manually for testing:
#   heroku run python3 worker.py --user-id=123 --task-id=abc -a your-app-name
