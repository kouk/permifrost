connection: "runners_db"

include: "runners.view.lkml"
label: "runners"

explore: {
  from: runners
  label: runners
  description: "GitLab CI Runners"
}
