When terminal config gets reset for whatever reason:
    source ~/.zshrc

To activate virtual environment:
    source .venv/bin/activate

To run python tests
    pytest -v
        -v adds the test breakdown

To start and stop docker containters:
    docker compose down
    docker compose up -d

To remove files that were already pushed to master
    git rm -r --cached <file_path>

To view past commits
    git log --oneline

To undo a certain number of commits
    git reset --soft HEAD~<number_of_commits>