class  SphereTestKit
  retailerStateDefs: [
    {key: "A", transitions: ["B"]}
    {key: "B", transitions: ["D"]}
    {key: "D", transitions: ["E"]}
    {key: "E", transitions: ["A"]}
  ]

  masterStateDefs: [
    {key: "MA", transitions: ["MB"]}
    {key: "MB", transitions: ["MC"]}
    {key: "MC", transitions: ["MD"]}
    {key: "MD", transitions: ["ME"]}
    {key: "ME", transitions: ["MA"]}
  ]

  constructor: (@sphere) ->

  setupProject: ->
    @configureStates()

  configureStates: ->
    Q.all [
      @sphere.ensureStates @retailerStateDefs
      @sphere.ensureStates @masterStateDefs
    ]
    .then (states) =>
      [@retailerStates, @masterStates] = states
      console.info "States configured"
    .fail (error) ->
      console.error "Failed to createStates"
      console.error error.stack