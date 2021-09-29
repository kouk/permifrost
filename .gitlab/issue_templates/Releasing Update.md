### Release Permifrost
**NOTE:** Releases should only be initiated by maintainers and owners of the Permifrost project

- [ ] Verify all necessary changes exist on your local master branch (i.e. `git pull origin master`)
- [ ] Verify that the CHANGELOG.md has correct, up-to-date changes for the release
- [ ] Create a bumpversion branch from the master branch. (i.e. `git checkout -b bumpversion`)
- [ ] From the root of the directory run `make suggest` to determine the recommended version update.
- [ ] Based on `make suggest` perform a release of `make release type=<patch|minor|major>` accordingly
- [ ] Generate an MR and notify the other maintainers to review the release.
- [ ] Go to the tags section in repo: https://gitlab.com/gitlab-data/permifrost/-/tags and select new tag in the top right.
- [ ] Create a new tag with the new version number, make sure to follow the naming convention to get this tagged correctly in pypi: `v<new full version label`. eg:`v0.12.0`
- [ ] Upon approval, merge the MR, ensure the pipelines complete successfully, these should push the new release to PyPi.

### Announce
- [ ] Send out an announcement in GitLab communications
- [ ] Announce in dbt Slack #tools-permifrost channel
