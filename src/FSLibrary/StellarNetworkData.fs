// Copyright 2019 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

module StellarNetworkData

open FSharp.Data
open stellar_dotnet_sdk
open StellarCoreSet
open StellarCoreCfg
open StellarMissionContext
open Logging

// This exists to work around a buggy interaction between FSharp.Data and
// dotnet 5.0.300: __SOURCE_DIRECTORY__ is not [<Literal>] anymore, but
// JsonProvider's ResolutionFolder argument needs to be.
[<Literal>]
let cwd = __SOURCE_DIRECTORY__

type HistoryArchiveState = JsonProvider<"json-type-samples/sample-stellar-history.json", ResolutionFolder=cwd>

let PubnetLatestHistoryArchiveState =
    "http://history.stellar.org/prd/core-live/core_live_001/.well-known/stellar-history.json"

let TestnetLatestHistoryArchiveState =
    "http://history.stellar.org/prd/core-testnet/core_testnet_001/.well-known/stellar-history.json"

type PubnetNode = JsonProvider<"json-type-samples/sample-network-data.json", SampleIsList=false, ResolutionFolder=cwd>
type Tier1PublicKey = JsonProvider<"json-type-samples/sample-keys.json", SampleIsList=false, ResolutionFolder=cwd>

let peerCountTier1 (random: System.Random) : int = random.Next(25, 81)

let peerCountNonTier1 (random: System.Random) : int = if random.Next(2) = 0 then 8 else random.Next(1, 71)

// For simplicity, we will make each new tier-1 organization contain exactly 3 nodes
// while scaling the pubnet.
let tier1OrgSize = 3

let extractEdges (graph: PubnetNode.Root array) : (string * string) array =
    let getEdgesFromNode (node: PubnetNode.Root) : (string * string) array =
        node.Peers
        |> Array.filter (fun peer -> peer < node.PublicKey) // This filter ensures that we add each edge exactly once.
        |> Array.map (fun peer -> (peer, node.PublicKey))

    graph |> Array.map getEdgesFromNode |> Array.reduce Array.append

let createAdjacencyMap (edgeSet: Set<string * string>) : Map<string, string list> =
    let edgeList : (string * string) list = Set.toList edgeSet

    let adjacencyList : (string * (string list)) list =
        edgeList
        |> List.append (List.map (fun (x, y) -> (y, x)) edgeList)
        |> List.groupBy fst
        |> List.map (fun (x, y) -> (x, List.map snd y))

    Map.ofList adjacencyList

// Add edges for `newNodes` and return a new adjacency map.
// The degree of each new node is determined by
// 1. Check if it's a tier-1 node by `tier1KeySet`.
// 2. Use peerCountTier1 or peerCountNonTier1 to decide.
//
// To preserve the degree of each existing node in `original`
// each new node's edges come from splitting an existing edge.
//
// More specifically, if we're adding a new node u,
// then we pick a random edge (a, b), remove (a, b) and add (a, u) and (u, b).
// We continue this process until u has a desired degree.
let addEdges
    (graph: PubnetNode.Root array)
    (newNodes: string array)
    (tier1KeySet: Set<string>)
    (random: System.Random)
    : Map<string, string list> =

    let edgeArray = extractEdges graph

    let edgeArrayList : ResizeArray<string * string> = new ResizeArray<string * string>(edgeArray)
    let mutable edgeSet : Set<string * string> = edgeArray |> Set.ofArray

    printfn "edgeSet has size %d" (Set.count edgeSet)

    printfn "%A" newNodes

    for u in newNodes do
        let mutable degreeRemaining =
            if Set.contains u tier1KeySet then
                peerCountTier1 random
            else
                peerCountNonTier1 random


        if degreeRemaining >= (Array.length graph) then
            // The chosen degree is larger than the given graph's cardinality.
            // This error is likely caused by passing the incorrect pubnet graph file.
            // We choose to throw here because
            // 1. This new node will have a degree significantly larger
            //    than any node in the original graph.
            // 2. The following algorithm will likely fail to find enough edges.
            // 3. If we adjust `degreeRemaining`, then the degree distribution will be impacted.
            failwith "The original graph is too small"

        let maxRetryCount = 100
        let mutable errorCount = 0

        while degreeRemaining > 0 do
            let index = random.Next(0, Set.count edgeSet)
            let (a, b) = edgeArrayList.[index]
            let maybeNewEdge1 = (min a u, max a u)
            let maybeNewEdge2 = (min b u, max b u)

            if (a <> u)
               && (b <> u)
               && (not (Set.contains maybeNewEdge1 edgeSet))
               && (not (Set.contains maybeNewEdge2 edgeSet)) then
                degreeRemaining <- degreeRemaining - 2
                // 1. Replace the current edge with maybeNewEdge1
                // 2. Append maybeNewEdge2
                //
                // This ensures that edgeList is exactly the list of all edges.
                edgeArrayList.[index] <- maybeNewEdge1
                edgeArrayList.Add(maybeNewEdge2)
                edgeSet <- edgeSet.Add(maybeNewEdge1)
                edgeSet <- edgeSet.Add(maybeNewEdge2)
                edgeSet <- edgeSet.Remove((a, b))
                errorCount <- 0
            else
                errorCount <- errorCount + 1

                if errorCount >= maxRetryCount then
                    failwith (sprintf "Unable to find an edge for %s after %d attempts" u maxRetryCount)

    printfn "%A" edgeArrayList
    createAdjacencyMap edgeSet


let FullPubnetCoreSets (context: MissionContext) (manualclose: bool) : CoreSet list =

    if context.pubnetData.IsNone then
        failwith "pubnet simulation requires --pubnet-data=<filename.json>"

    if context.tier1Keys.IsNone then
        failwith "pubnet simulation requires --tier1-keys=<filename.json>"

    let allPubnetNodes : PubnetNode.Root array = PubnetNode.Load(context.pubnetData.Value)

    // A Random object with a fixed seed.
    let random = System.Random 0

    if context.tier1NodesToAdd % tier1OrgSize <> 0 then
        failwith (sprintf "The number of new tier-1 nodes must be a multiple of %d" tier1OrgSize)

    let newTier1Nodes =
        [ for i in 1 .. context.tier1NodesToAdd ->
              PubnetNode.Parse(
                  sprintf
                      """ [{ "publicKey": "TIER1-NODE-%d", "sb_homeDomain": "home.domain.%d" }] """
                      i
                      ((i - 1) / tier1OrgSize)
              ).[0] ]
        |> Array.ofList

    let newNonTier1Nodes =
        [ for i in 1 .. context.nonTier1NodesToAdd ->
              PubnetNode.Parse(sprintf """ [{ "publicKey": "NON-TIER1-NODE-%d" }] """ i).[0] ]
        |> Array.ofList

    printfn "newTier1Nodes %A" newTier1Nodes
    printfn "newNonTier1Nodes %A" newNonTier1Nodes

    let tier1KeySet : Set<string> =
        let newTier1Keys = Array.map (fun (n: PubnetNode.Root) -> n.PublicKey) newTier1Nodes in

        Tier1PublicKey.Load(context.tier1Keys.Value)
        |> Array.map (fun n -> n.PublicKey)
        |> Array.append newTier1Keys
        |> Set.ofArray


    // Shuffle the nodes to ensure that the order will
    // not affect the outcome of the scaling algorithm.
    let newNodes =
        Array.append newTier1Nodes newNonTier1Nodes
        |> Array.sortBy (fun _ -> random.Next())

    let allPubnetNodes = allPubnetNodes |> Array.append newTier1Nodes |> Array.append newNonTier1Nodes

    // For each pubkey in the pubnet, we map it to an actual KeyPair (with a private
    // key) to use in the simulation. It's important to keep these straight! The keys
    // in the pubnet are _not_ used in the simulation. They are used as node identities
    // throughout the rest of this function, as strings called "pubkey", but should not
    // appear in the final CoreSets we're building.
    let mutable pubnetKeyToSimKey : Map<string, KeyPair> =
        Array.map (fun (n: PubnetNode.Root) -> (n.PublicKey, KeyPair.Random())) allPubnetNodes
        |> Map.ofArray

    // Not every pubkey used in the qsets is represented in the base set of pubnet nodes,
    // so we lazily extend the set here with new mappings as we discover new pubkeys.
    let getSimKey (pubkey: string) : KeyPair =
        match pubnetKeyToSimKey.TryFind pubkey with
        | Some (k) -> k
        | None ->
            let k = KeyPair.Random()
            pubnetKeyToSimKey <- pubnetKeyToSimKey.Add(pubkey, k)
            k

    let edgeMap =
        addEdges allPubnetNodes (Array.map (fun (n: PubnetNode.Root) -> n.PublicKey) newNodes) tier1KeySet random

    // First we partition nodes in the network into those that have home domains and
    // those that do not. We call the former "org" nodes and the latter "misc" nodes.
    let orgNodes, miscNodes =
        Array.partition (fun (n: PubnetNode.Root) -> n.SbHomeDomain.IsSome) allPubnetNodes

    // We then trim down the set of misc nodes so that they fit within simulation
    // size limit passed. If we can't even fit the org nodes, we fail here.
    let miscNodes =
        (let numOrgNodes = Array.length orgNodes
         let numMiscNodes = Array.length miscNodes

         let _ =
             if numOrgNodes > context.networkSizeLimit then
                 failwith "simulated network size limit too small to fit org nodes"

         let takeMiscNodes = min (context.networkSizeLimit - numOrgNodes) numMiscNodes
         Array.take takeMiscNodes miscNodes)

    let allPubnetNodes = Array.append orgNodes miscNodes
    let _ = assert ((Array.length allPubnetNodes) <= context.networkSizeLimit)

    let allPubnetNodeKeys =
        Array.map (fun (n: PubnetNode.Root) -> n.PublicKey) allPubnetNodes
        |> Set.ofArray

    LogInfo "SimulatePubnet will run with %d nodes" (Array.length allPubnetNodes)

    // We then group the org nodes by their home domains. The domain names are drawn
    // from the HomeDomains of the public network but with periods replaced with dashes,
    // and lowercased, so for example keybase.io turns into keybase-io.
    let groupedOrgNodes : (HomeDomainName * PubnetNode.Root array) array =
        Array.groupBy
            (fun (n: PubnetNode.Root) ->
                let cleanOrgName = n.SbHomeDomain.Value.Replace('.', '-')
                let lowercase = cleanOrgName.ToLower()
                HomeDomainName lowercase)
            orgNodes


    // Then build a map from accountID to HomeDomainName and index-within-domain
    // for each org node.
    let orgNodeHomeDomains : Map<string, (HomeDomainName * int)> =
        Array.collect
            (fun (hdn: HomeDomainName, nodes: PubnetNode.Root array) ->
                Array.mapi (fun (i: int) (n: PubnetNode.Root) -> (n.PublicKey, (hdn, i))) nodes)
            groupedOrgNodes
        |> Map.ofArray

    // When we don't have real HomeDomainNames (eg. for misc nodes) we use a
    // node-XXXXXX name with the XXXXXX as the lowercased first 6 digits of the
    // pubkey. Lowercase because kubernetes resource objects have to have all
    // lowercase names.
    let anonymousNodeName (pubkey: string) : string =
        let simkey = (getSimKey pubkey).Address
        let extract = simkey.Substring(0, 6)
        let lowercase = extract.ToLower()
        "node-" + lowercase

    // Return either HomeDomainName name or a node-XXXXXX name derived from the pubkey.
    let homeDomainNameForKey (pubkey: string) : HomeDomainName =
        match orgNodeHomeDomains.TryFind pubkey with
        | Some (s, _) -> s
        | None -> HomeDomainName(anonymousNodeName pubkey)

    // Return either HomeDomainName-$i name or a node-XXXXXX-0 name derived from the pubkey.
    let peerShortNameForKey (pubkey: string) : PeerShortName =
        match orgNodeHomeDomains.TryFind pubkey with
        | Some (s, i) -> PeerShortName(sprintf "%s-%d" s.StringName i)
        | None -> PeerShortName((anonymousNodeName pubkey) + "-0")

    let pubKeysToValidators (pubKeys: string array) : Map<PeerShortName, KeyPair> =
        pubKeys
        |> Array.map (fun k -> (peerShortNameForKey k, getSimKey k))
        |> Map.ofArray

    let defaultQuorum : QuorumSet =
        let tier1NodesGroupedByHomeDomain : (string array) array =
            allPubnetNodes
            |> Array.filter (fun (n: PubnetNode.Root) -> Set.contains n.PublicKey tier1KeySet)
            |> Array.filter (fun (n: PubnetNode.Root) -> n.SbHomeDomain.IsSome)
            |> Array.groupBy (fun (n: PubnetNode.Root) -> n.SbHomeDomain.Value)
            |> Array.map
                (fun (domain: string, nodes: PubnetNode.Root []) ->
                    Array.map (fun (n: PubnetNode.Root) -> n.PublicKey) nodes)

        let orgToQSet (org: string array) : QuorumSet =
            let sz = Array.length org
            // The largest number of non-Byzantine failures we can sustain.
            // In other words, we want the largest f such that 2f + 1 <= nOrgs.
            let nonByzantine = (sz - 1) / 2

            { thresholdPercent = Some(percentOfThreshold sz (sz - nonByzantine))
              validators = pubKeysToValidators org
              innerQuorumSets = Array.empty }

        let nOrgs = Array.length tier1NodesGroupedByHomeDomain

        // The largest number of Byzantine failures we can sustain.
        // In other words, we want the largest f such that 3f + 1 <= nOrgs.
        let byzantine : int = (nOrgs - 1) / 3

        { thresholdPercent = Some(percentOfThreshold nOrgs (nOrgs - byzantine))
          validators = Map.empty
          innerQuorumSets = Array.map orgToQSet tier1NodesGroupedByHomeDomain }

    let pubnetOpts =
        { CoreSetOptions.GetDefault context.image with
              accelerateTime = false
              historyNodes = Some([])
              // We need to use a synchronized startup delay
              // for networks as large as this, otherwise it loses
              // sync before all the nodes are online.
              syncStartupDelay = Some(30)
              invariantChecks = AllInvariantsExceptBucketConsistencyChecks
              dumpDatabase = false }

    // Sorted list of known geolocations.
    // We can choose an arbitrary geolocation such that the distribution follows that of the given data
    // by uniformly randomly selecting an element from this array.
    // Since this is sorted, we will have the same assignments across different runs
    // as long as the random function is persistent.
    let geoLocations : GeoLoc array =
        allPubnetNodes
        |> Array.filter (fun (n: PubnetNode.Root) -> n.SbGeoData.IsSome)
        |> Array.map
            (fun (n: PubnetNode.Root) ->
                { lat = float n.SbGeoData.Value.Latitude
                  lon = float n.SbGeoData.Value.Longitude })
        |> Seq.ofArray
        |> Seq.sortBy (fun x -> x.lat, x.lon)
        |> Array.ofSeq

    // If the given node has geolocation info, use it.
    // Otherwise, pseudo-randomly select one from the list of geolocations that we are aware of.
    // Since the assignment depends on the pubnet public key,
    // this assignment is persistent across different runs.
    let getGeoLocOrDefault (n: PubnetNode.Root) : GeoLoc =
        match n.SbGeoData with
        | Some geoData -> { lat = float geoData.Latitude; lon = float geoData.Longitude }
        | None ->
            let len = Array.length geoLocations
            // A deterministic, fairly elementary hashing function that adds the ASCII code
            // of each character.
            // All we need is a deterministic, mostly randomized mapping.
            let h = n.PublicKey |> Seq.sumBy int
            geoLocations.[h % len]

    let makeCoreSetWithExplicitKeys (hdn: HomeDomainName) (options: CoreSetOptions) (keys: KeyPair array) =
        { name = CoreSetName(hdn.StringName)
          options = options
          keys = keys
          live = true }

    let preferredPeersMapForAllNodes : Map<byte [], byte [] list> =
        let getSimPubKey (k: string) = (getSimKey k).PublicKey

        allPubnetNodes
        |> Array.map
            (fun (n: PubnetNode.Root) ->
                let key = getSimPubKey n.PublicKey

                let peers =
                    Map.find n.PublicKey edgeMap
                    |>
                    // This filtering is necessary since we intentionally remove some nodes
                    // using networkSizeLimit.
                    List.filter (fun (k: string) -> Set.contains k allPubnetNodeKeys)
                    |> List.map getSimPubKey

                (key, peers))
        |> Map.ofArray

    let keysToPreferredPeersMap (keys: KeyPair array) =
        keys
        |> Array.map (fun (k: KeyPair) -> (k.PublicKey, Map.find k.PublicKey preferredPeersMapForAllNodes))
        |> Map.ofArray

    let miscCoreSets : CoreSet array =
        Array.mapi
            (fun (_: int) (n: PubnetNode.Root) ->
                let hdn = homeDomainNameForKey n.PublicKey
                let keys = [| getSimKey n.PublicKey |]

                let coreSetOpts =
                    { pubnetOpts with
                          nodeCount = 1
                          quorumSet = ExplicitQuorum defaultQuorum
                          tier1 = Some(Set.contains n.PublicKey tier1KeySet)
                          nodeLocs = Some [ getGeoLocOrDefault n ]
                          preferredPeersMap = Some(keysToPreferredPeersMap keys) }

                let shouldWaitForConsensus = manualclose
                let coreSetOpts = coreSetOpts.WithWaitForConsensus shouldWaitForConsensus
                makeCoreSetWithExplicitKeys hdn coreSetOpts keys)
            miscNodes

    let orgCoreSets : CoreSet array =
        Array.map
            (fun (hdn: HomeDomainName, nodes: PubnetNode.Root array) ->
                assert (nodes.Length <> 0)
                let nodeList = List.ofArray nodes
                let keys = Array.map (fun (n: PubnetNode.Root) -> getSimKey n.PublicKey) nodes

                let coreSetOpts =
                    { pubnetOpts with
                          nodeCount = Array.length nodes
                          quorumSet = ExplicitQuorum defaultQuorum
                          tier1 = Some(Set.contains nodes.[0].PublicKey tier1KeySet)
                          nodeLocs = Some(List.map getGeoLocOrDefault nodeList)
                          preferredPeersMap = Some(keysToPreferredPeersMap keys) }

                let shouldWaitForConsensus = manualclose
                let coreSetOpts = coreSetOpts.WithWaitForConsensus shouldWaitForConsensus
                makeCoreSetWithExplicitKeys hdn coreSetOpts keys)
            groupedOrgNodes

    Array.append miscCoreSets orgCoreSets |> List.ofArray


let GetLatestPubnetLedgerNumber _ : int =
    let has = HistoryArchiveState.Load(PubnetLatestHistoryArchiveState)
    has.CurrentLedger

let GetLatestTestnetLedgerNumber _ : int =
    let has = HistoryArchiveState.Load(TestnetLatestHistoryArchiveState)
    has.CurrentLedger


let PubnetGetCommands =
    [ PeerShortName "core_live_001", "curl -sf http://history.stellar.org/prd/core-live/core_live_001/{0} -o {1}"
      PeerShortName "core_live_002", "curl -sf http://history.stellar.org/prd/core-live/core_live_002/{0} -o {1}"
      PeerShortName "core_live_003", "curl -sf http://history.stellar.org/prd/core-live/core_live_003/{0} -o {1}" ]
    |> Map.ofList

let PubnetQuorum : QuorumSet =
    { thresholdPercent = None
      validators =
          [ PeerShortName "core_live_001",
            KeyPair.FromAccountId("GCGB2S2KGYARPVIA37HYZXVRM2YZUEXA6S33ZU5BUDC6THSB62LZSTYH")
            PeerShortName "core_live_002",
            KeyPair.FromAccountId("GCM6QMP3DLRPTAZW2UZPCPX2LF3SXWXKPMP3GKFZBDSF3QZGV2G5QSTK")
            PeerShortName "core_live_003",
            KeyPair.FromAccountId("GABMKJM6I25XI4K7U6XWMULOUQIQ27BCTMLS6BYYSOWKTBUXVRJSXHYQ") ]
          |> Map.ofList
      innerQuorumSets = [||] }

let PubnetPeers =
    [ PeerDnsName "core-live4.stellar.org"
      PeerDnsName "core-live5.stellar.org"
      PeerDnsName "core-live6.stellar.org" ]

let PubnetCoreSetOptions (image: string) =
    { CoreSetOptions.GetDefault image with
          quorumSet = ExplicitQuorum PubnetQuorum
          historyGetCommands = PubnetGetCommands
          peersDns = PubnetPeers
          accelerateTime = false
          initialization = { CoreSetInitialization.Default with waitForConsensus = true }
          dumpDatabase = false }

let TestnetGetCommands =
    [ PeerShortName "core_testnet_001",
      "curl -sf http://history.stellar.org/prd/core-testnet/core_testnet_001/{0} -o {1}"
      PeerShortName "core_testnet_002",
      "curl -sf http://history.stellar.org/prd/core-testnet/core_testnet_002/{0} -o {1}"
      PeerShortName "core_testnet_003",
      "curl -sf http://history.stellar.org/prd/core-testnet/core_testnet_003/{0} -o {1}" ]
    |> Map.ofList

let TestnetQuorum : QuorumSet =
    { thresholdPercent = None
      validators =
          [ PeerShortName "core_testnet_001",
            KeyPair.FromAccountId("GDKXE2OZMJIPOSLNA6N6F2BVCI3O777I2OOC4BV7VOYUEHYX7RTRYA7Y")
            PeerShortName "core_testnet_002",
            KeyPair.FromAccountId("GCUCJTIYXSOXKBSNFGNFWW5MUQ54HKRPGJUTQFJ5RQXZXNOLNXYDHRAP")
            PeerShortName "core_testnet_003",
            KeyPair.FromAccountId("GC2V2EFSXN6SQTWVYA5EPJPBWWIMSD2XQNKUOHGEKB535AQE2I6IXV2Z") ]
          |> Map.ofList
      innerQuorumSets = [||] }

let TestnetPeers =
    [ PeerDnsName "core-testnet1.stellar.org"
      PeerDnsName "core-testnet2.stellar.org"
      PeerDnsName "core-testnet3.stellar.org" ]

let TestnetCoreSetOptions (image: string) =
    { CoreSetOptions.GetDefault image with
          quorumSet = ExplicitQuorum TestnetQuorum
          historyGetCommands = TestnetGetCommands
          peersDns = TestnetPeers
          accelerateTime = false
          initialization = { CoreSetInitialization.Default with waitForConsensus = true }
          dumpDatabase = false }
