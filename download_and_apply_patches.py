#!/usr/bin/env python3
import subprocess

CONST_PATCH_FOLDER_PATH = './patches'

def get_last_commit_message() -> str:
    command = ['git', 'log', '-1', '--pretty=%B']
    return subprocess.check_output(command).decode('utf-8')

def download_patches(download_path: str, URL: str) -> None:
    None

def apply_patches_from(patch_path: str) -> None:
    None

'''
download_and_apply_patches: &download_and_apply_patches
  download_and_apply_patches_script:
    python3 download_and_apply_patches.py
'''

def main():
    # CFBot will:

    # 1 - Edit .cirrus.yml file
    # .... It will add new script(example is above) for applying patches and
    # .... edit all other tasks like

    # .... task:
    # ....  <<: *download_and_apply_patches

    # .... So, tasks will apply patches as a first step
    # .... ^^ I am not sure how to achieve that but at least traversing file should work

    # 2 - Put current python script(download_and_apply_patches) to the postgres codebase

    # 3 - Commit message will be discussion URL

    # The good thing is SanityCheck task will run this newly created script
    # so if there is a failure while applying a patch, other tasks won't run
    # but if there isn't a failure; then it will download patches {number_of_tasks} times


    # So, when the branch is published or updated
    # First, this newly created script will run.
    # Then, it will read discussion URL from commit message
    # download patches using discussion URL
    # apply patches
    # So, all of the tests will run after patch is applied

    # URL will be get from commit message
    discussion_url = get_last_commit_message()

    # Patches will be downloaded using discussion url
    download_patches(CONST_PATCH_FOLDER_PATH, discussion_url)

    # Pathces will be applied
    apply_patches_from(CONST_PATCH_FOLDER_PATH)

if __name__ == "__main__":
    main()