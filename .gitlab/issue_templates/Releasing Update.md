### Release Permifrost
**NOTE:** Releases should only be initiated by maintainers and owners of the Permifrost project

- [ ] Verify all necessary changes exist on your local master branch (i.e. `git pull origin master`)
- [ ] Verify that the CHANGELOG.md has correct and up-to-date changes for the release
- [ ] Create a bumpversion branch from the master branch (i.e. `git checkout -b bumpversion`)
- [ ] From the root of the directory, run `make suggest` to determine the recommended version update
- [ ] Based on the prior step, run `make release type=<patch|minor|major>` based on the type of semantic release needed
- [ ] Push changes to the remote (i.e. `git push origin bumpversion`)
- [ ] Generate an MR and notify the other maintainers to review the release
    - **NOTE:** For changes required through the review process. Ensure you delete the local tag `git tag -d v<x.x.x>`, commit the new changes, and generate a new tag with `git tag -a v<x.x.x> -m "Bump version: <current> -> <update>"`
- [ ] Upon approval, merge the MR and run `git push origin --tags` locally. After CI completes, the new release should now be on PyPi

### Announce
- [ ] Send out an announcement in GitLab communications
- [ ] Announce in dbt Slack #tools-permifrost channel
