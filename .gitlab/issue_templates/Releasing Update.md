### Release Permifrost
**NOTE:** Releases should only be initiated by maintainers and owners of the Permifrost project

- [ ] Verify all necessary changes exist on your local master branch (i.e. `git pull origin master`)
- [ ] Verify that the CHANGELOG.md has correct and up-to-date changes for the release
- [ ] Create a bumpversion branch from the master branch. (i.e. `git checkout -b bumpversion`)
- [ ] From the root of the directory, run `make suggest` to determine the recommended version update
- [ ] Based on the prior step, run `make release type=<patch|minor|major>` based on the type of semantic release needed
- [ ] Generate an MR and notify the other maintainers to review the release
- [ ] Upon approval, merge the MR and ensure the pipelines complete successfully. The new release should now be on PyPi

### Announce
- [ ] Send out an announcement in GitLab communications
- [ ] Announce in dbt Slack #tools-permifrost channel
