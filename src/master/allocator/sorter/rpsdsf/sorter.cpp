// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "master/allocator/sorter/rpsdsf/sorter.hpp"

#include <set>
#include <string>
#include <vector>

#include <mesos/mesos.hpp>
#include <mesos/resources.hpp>
#include <mesos/values.hpp>

#include <process/pid.hpp>

#include <stout/check.hpp>
#include <stout/foreach.hpp>
#include <stout/hashmap.hpp>
#include <stout/option.hpp>
#include <stout/strings.hpp>

using std::set;
using std::string;
using std::vector;

using process::UPID;

namespace mesos {
namespace internal {
namespace master {
namespace allocator {


RPSDSFSorter::RPSDSFSorter()
  : root(new Node("", Node::INTERNAL, nullptr)) {}


RPSDSFSorter::RPSDSFSorter(
    const UPID& allocator,
    const string& metricsPrefix)
  : root(new Node("", Node::INTERNAL, nullptr)),
    metrics(RPSDSFMetrics(allocator, *this, metricsPrefix)) {}


RPSDSFSorter::~RPSDSFSorter()
{
  delete root;
}


bool RPSDSFSorter::residual() {
  return true;
}

void RPSDSFSorter::initialize(
    const Option<set<string>>& _fairnessExcludeResourceNames)
{
  fairnessExcludeResourceNames = _fairnessExcludeResourceNames;
}


void RPSDSFSorter::add(const string& clientPath)
{
  vector<string> pathElements = strings::tokenize(clientPath, "/");
  CHECK(!pathElements.empty());

  Node* current = root;
  Node* lastCreatedNode = nullptr;

  // Traverse the tree to add new nodes for each element of the path,
  // if that node doesn't already exist (similar to `mkdir -p`).
  foreach (const string& element, pathElements) {
    Node* node = nullptr;

    foreach (Node* child, current->children) {
      if (child->name == element) {
        node = child;
        break;
      }
    }

    if (node != nullptr) {
      current = node;
      continue;
    }

    // We didn't find `element`, so add a new child to `current`.
    //
    // If adding this child would result in turning `current` from a
    // leaf node into an internal node, we need to create an
    // additional child node: `current` must have been associated with
    // a client and clients must always be associated with leaf nodes.
    if (current->isLeaf()) {
      Node* parent = CHECK_NOTNULL(current->parent);

      parent->removeChild(current);

      // Create a node under `parent`. This internal node will take
      // the place of `current` in the tree.
      Node* internal = new Node(current->name, Node::INTERNAL, parent);
      parent->addChild(internal);
      internal->allocation = current->allocation;

      CHECK_EQ(current->path, internal->path);

      // Update `current` to become a virtual leaf node and a child of
      // `internal`.
      current->name = ".";
      current->parent = internal;
      current->path = strings::join("/", parent->path, current->name);

      internal->addChild(current);

      CHECK_EQ(internal->path, current->clientPath());

      current = internal;
    }

    // Now actually add a new child to `current`.
    Node* newChild = new Node(element, Node::INTERNAL, current);
    current->addChild(newChild);

    current = newChild;
    lastCreatedNode = newChild;
  }

  CHECK(current->kind == Node::INTERNAL);

  // `current` is the node associated with the last element of the
  // path. If we didn't add `current` to the tree above, create a leaf
  // node now. For example, if the tree contains "a/b" and we add a
  // new client "a", we want to create a new leaf node "a/." here.
  if (current != lastCreatedNode) {
    Node* newChild = new Node(".", Node::INACTIVE_LEAF, current);
    current->addChild(newChild);
    current = newChild;
  } else {
    // If we created `current` in the loop above, it was marked an
    // `INTERNAL` node. It should actually be an inactive leaf node.
    current->kind = Node::INACTIVE_LEAF;

    // `current` has changed from an internal node to an inactive
    // leaf, so remove and re-add it to its parent. This moves it to
    // the end of the parent's list of children.
    CHECK_NOTNULL(current->parent);

    current->parent->removeChild(current);
    current->parent->addChild(current);
  }

  // `current` is the newly created node associated with the last
  // element of the path. `current` should be an inactive leaf node.
  CHECK(current->children.empty());
  CHECK(current->kind == Node::INACTIVE_LEAF);

  // Add a new entry to the lookup table. The full path of the newly
  // added client should not already exist in `clients`.
  CHECK_EQ(clientPath, current->clientPath());
  CHECK(!clients.contains(clientPath));

  clients[clientPath] = current;

  // TODO(neilc): Avoid dirtying the tree in some circumstances.
  dirty = true;

  if (metrics.isSome()) {
    metrics->add(clientPath);
  }
}


void RPSDSFSorter::remove(const string& clientPath)
{
  Node* current = CHECK_NOTNULL(find(clientPath));

  // Save a copy of the leaf node's allocated resources, because we
  // destroy the leaf node below.
  const hashmap<SlaveID, Resources> leafAllocation =
    current->allocation.resources;

  // Remove the lookup table entry for the client.
  CHECK(clients.contains(clientPath));
  clients.erase(clientPath);

  // To remove a client from the tree, we have to do two things:
  //
  //   (1) Update the tree structure to reflect the removal of the
  //       client. This means removing the client's leaf node, then
  //       walking back up the tree to remove any internal nodes that
  //       are now unnecessary.
  //
  //   (2) Update allocations of ancestor nodes to reflect the removal
  //       of the client.
  //
  // We do both things at once: find the leaf node, remove it, and
  // walk up the tree, updating ancestor allocations and removing
  // ancestors when possible.
  while (current != root) {
    Node* parent = CHECK_NOTNULL(current->parent);

    // Update `parent` to reflect the fact that the resources in the
    // leaf node are no longer allocated to the subtree rooted at
    // `parent`. We skip `root`, because we never update the
    // allocation made to the root node.
    if (parent != root) {
      foreachpair (const SlaveID& slaveId,
                   const Resources& resources,
                   leafAllocation) {
        parent->allocation.subtract(slaveId, resources);
      }
    }

    if (current->children.empty()) {
      parent->removeChild(current);
      delete current;
    } else if (current->children.size() == 1) {
      // If `current` has only one child that was created to
      // accommodate inserting `clientPath` (see `RPSDSFSorter::add()`),
      // we can remove the child node and turn `current` back into a
      // leaf node.
      Node* child = *(current->children.begin());

      if (child->name == ".") {
        CHECK(child->isLeaf());
        CHECK(clients.contains(current->path));
        CHECK_EQ(child, clients.at(current->path));

        current->kind = child->kind;
        current->removeChild(child);

        // `current` has changed kind (from `INTERNAL` to a leaf,
        // which might be active or inactive). Hence we might need to
        // change its position in the `children` list.
        if (current->kind == Node::INTERNAL) {
          CHECK_NOTNULL(current->parent);

          current->parent->removeChild(current);
          current->parent->addChild(current);
        }

        clients[current->path] = current;

        delete child;
      }
    }

    current = parent;
  }

  // TODO(neilc): Avoid dirtying the tree in some circumstances.
  dirty = true;

  if (metrics.isSome()) {
    metrics->remove(clientPath);
  }
}


void RPSDSFSorter::activate(const string& clientPath)
{
  Node* client = CHECK_NOTNULL(find(clientPath));

  if (client->kind == Node::INACTIVE_LEAF) {
    client->kind = Node::ACTIVE_LEAF;

    // `client` has been activated, so move it to the beginning of its
    // parent's list of children. We mark the tree dirty, so that the
    // client's share is updated correctly and it is sorted properly.
    //
    // TODO(neilc): We could instead calculate share here and insert
    // the client into the appropriate place here, which would avoid
    // dirtying the whole tree.
    CHECK_NOTNULL(client->parent);

    client->parent->removeChild(client);
    client->parent->addChild(client);

    dirty = true;
  }
}


void RPSDSFSorter::deactivate(const string& clientPath)
{
  Node* client = CHECK_NOTNULL(find(clientPath));

  if (client->kind == Node::ACTIVE_LEAF) {
    client->kind = Node::INACTIVE_LEAF;

    // `client` has been deactivated, so move it to the end of its
    // parent's list of children.
    CHECK_NOTNULL(client->parent);

    client->parent->removeChild(client);
    client->parent->addChild(client);
  }
}


void RPSDSFSorter::updateWeight(const string& path, double weight)
{
  weights[path] = weight;

  // TODO(neilc): Avoid dirtying the tree in some circumstances.
  dirty = true;
}


void RPSDSFSorter::allocated(
    const string& clientPath,
    const SlaveID& slaveId,
    const Resources& resources)
{
  Node* current = CHECK_NOTNULL(find(clientPath));

  // NOTE: We don't currently update the `allocation` for the root
  // node. This is debatable, but the current implementation doesn't
  // require looking at the allocation of the root node.
  while (current != root) {
    current->allocation.add(slaveId, resources);
    current = CHECK_NOTNULL(current->parent);
  }

  // TODO(neilc): Avoid dirtying the tree in some circumstances.
  dirty = true;
}


void RPSDSFSorter::update(
    const string& clientPath,
    const SlaveID& slaveId,
    const Resources& oldAllocation,
    const Resources& newAllocation)
{
  // TODO(bmahler): Check invariants between old and new allocations.
  // Namely, the roles and quantities of resources should be the same!
  // Otherwise, we need to ensure we re-calculate the shares, as
  // is being currently done, for safety.

  Node* current = CHECK_NOTNULL(find(clientPath));

  // NOTE: We don't currently update the `allocation` for the root
  // node. This is debatable, but the current implementation doesn't
  // require looking at the allocation of the root node.
  while (current != root) {
    current->allocation.update(slaveId, oldAllocation, newAllocation);
    current = CHECK_NOTNULL(current->parent);
  }

  // Just assume the total has changed, per the TODO above.
  dirty = true;
}


void RPSDSFSorter::unallocated(
    const string& clientPath,
    const SlaveID& slaveId,
    const Resources& resources)
{
  Node* current = CHECK_NOTNULL(find(clientPath));

  // NOTE: We don't currently update the `allocation` for the root
  // node. This is debatable, but the current implementation doesn't
  // require looking at the allocation of the root node.
  while (current != root) {
    current->allocation.subtract(slaveId, resources);
    current = CHECK_NOTNULL(current->parent);
  }

  // TODO(neilc): Avoid dirtying the tree in some circumstances.
  dirty = true;
}


const hashmap<SlaveID, Resources>& RPSDSFSorter::allocation(
    const string& clientPath) const
{
  const Node* client = CHECK_NOTNULL(find(clientPath));
  return client->allocation.resources;
}


const Resources& RPSDSFSorter::allocationScalarQuantities(
    const string& clientPath) const
{
  const Node* client = CHECK_NOTNULL(find(clientPath));
  return client->allocation.scalarQuantities;
}


hashmap<string, Resources> RPSDSFSorter::allocation(
    const SlaveID& slaveId) const
{
  hashmap<string, Resources> result;

  // We want to find the allocation that has been made to each client
  // on a particular `slaveId`. Rather than traversing the tree
  // looking for leaf nodes (clients), we can instead just iterate
  // over the `clients` hashmap.
  //
  // TODO(jmlvanre): We can index the allocation by slaveId to make
  // this faster.  It is a tradeoff between speed vs. memory. For now
  // we use existing data structures.
  foreachvalue (const Node* client, clients) {
    if (client->allocation.resources.contains(slaveId)) {
      // It is safe to use `at()` here because we've just checked the
      // existence of the key. This avoids unnecessary copies.
      string path = client->clientPath();
      CHECK(!result.contains(path));
      result.emplace(path, client->allocation.resources.at(slaveId));
    }
  }

  return result;
}


Resources RPSDSFSorter::allocation(
    const string& clientPath,
    const SlaveID& slaveId) const
{
  const Node* client = CHECK_NOTNULL(find(clientPath));

  if (client->allocation.resources.contains(slaveId)) {
    return client->allocation.resources.at(slaveId);
  }

  return Resources();
}


const Resources& RPSDSFSorter::totalScalarQuantities() const
{
  return total_.scalarQuantities;
}


void RPSDSFSorter::add(const SlaveID& slaveId, const Resources& resources)
{
  if (!resources.empty()) {
    // Add shared resources to the total quantities when the same
    // resources don't already exist in the total.
    const Resources newShared = resources.shared()
      .filter([this, slaveId](const Resource& resource) {
        return !total_.resources[slaveId].contains(resource);
      });

    total_.resources[slaveId] += resources;

    const Resources scalarQuantities =
      (resources.nonShared() + newShared).createStrippedScalarQuantity();

    total_.scalarQuantities += scalarQuantities;

    foreach (const Resource& resource, scalarQuantities) {
      total_.totals[resource.name()] += resource.scalar();
    }

    // We have to recalculate all shares when the total resources
    // change, but we put it off until `sort` is called so that if
    // something else changes before the next allocation we don't
    // recalculate everything twice.
    dirty = true;
  }
}


void RPSDSFSorter::remove(const SlaveID& slaveId, const Resources& resources)
{
  if (!resources.empty()) {
    CHECK(total_.resources.contains(slaveId));
    CHECK(total_.resources[slaveId].contains(resources))
      << total_.resources[slaveId] << " does not contain " << resources;

    total_.resources[slaveId] -= resources;

    // Remove shared resources from the total quantities when there
    // are no instances of same resources left in the total.
    const Resources absentShared = resources.shared()
      .filter([this, slaveId](const Resource& resource) {
        return !total_.resources[slaveId].contains(resource);
      });

    const Resources scalarQuantities =
      (resources.nonShared() + absentShared).createStrippedScalarQuantity();

    foreach (const Resource& resource, scalarQuantities) {
      total_.totals[resource.name()] -= resource.scalar();
    }

    CHECK(total_.scalarQuantities.contains(scalarQuantities));
    total_.scalarQuantities -= scalarQuantities;

    if (total_.resources[slaveId].empty()) {
      total_.resources.erase(slaveId);
    }

    dirty = true;
  }
}

vector<string> RPSDSFSorter::sort() {
  vector<string> result;
  return result;
}

vector<string> RPSDSFSorter::sort(const SlaveID& slaveId) {
  // TODO(yuquanshan): since we need to re-sort when slave is changed,
  // the global (cross-slave) variable "dirty" is useless in our case,
  // so change "dirty" to "true" in if (.). Alternatively, we can add
  // another variable to, say, root node, which indicates which slave
  // it tracks last time. As a result, if (.) would contain two
  // conditions: dirty || slaveId.value() != root->lastSlave.value().
  if (true) {
    std::function<void (Node*)> sortTree =
        [this, &slaveId, &sortTree](Node* node) {
      // Inactive leaves are always stored at the end of the
      // `children` vector; this means that as soon as we see an
      // inactive leaf, we can stop calculating shares, and we only
      // need to sort the prefix of the vector before that point.
      auto childIter = node->children.begin();

      while (childIter != node->children.end()) {
        Node* child = *childIter;

        if (child->kind == Node::INACTIVE_LEAF) {
          break;
        }

        child->share = calculateShare(child, slaveId);
        ++childIter;
      }

      std::sort(node->children.begin(),
                childIter,
                RPSDSFSorter::Node::compareRPSDSF);

      foreach (Node* child, node->children) {
        if (child->kind == Node::INTERNAL) {
          sortTree(child);
        } else if (child->kind == Node::INACTIVE_LEAF) {
          break;
        }
      }
    };

    sortTree(root);

    dirty = false;
  }

  // Return all active leaves in the tree via pre-order traversal.
  // The children of each node are already sorted in RPSDSF order, with
  // inactive leaves sorted after active leaves and internal nodes.
  vector<string> result;

  std::function<void (const Node*)> listClients =
      [&listClients, &result](const Node* node) {
    foreach (const Node* child, node->children) {
      switch (child->kind) {
        case Node::ACTIVE_LEAF:
          result.push_back(child->clientPath());
          break;

        case Node::INACTIVE_LEAF:
          // As soon as we see the first inactive leaf, we can stop
          // iterating over the current node's list of children.
          return;

        case Node::INTERNAL:
          listClients(child);
          break;
      }
    }
  };

  listClients(root);

  return result;
}

std::priority_queue<std::pair<std::string, double>,
                              std::vector< std::pair<std::string, double> >,
                              Sorter::ComparePair>
RPSDSFSorter::yieldHeap(const SlaveID& slaveId) {
  // TODO(yuquanshan): since we need to re-sort when slave is changed,
  // the global (cross-slave) variable "dirty" is useless in our case,
  // so change "dirty" to "true" in if (.). Alternatively, we can add
  // another variable to, say, root node, which indicates which slave
  // it tracks last time. As a result, if (.) would contain two
  // conditions: dirty || slaveId.value() != root->lastSlave.value().
  if (true) {
    std::function<void (Node*)> sortTree =
        [this, &slaveId, &sortTree](Node* node) {
      // Inactive leaves are always stored at the end of the
      // `children` vector; this means that as soon as we see an
      // inactive leaf, we can stop calculating shares, and we only
      // need to sort the prefix of the vector before that point.
      auto childIter = node->children.begin();

      while (childIter != node->children.end()) {
        Node* child = *childIter;

        if (child->kind == Node::INACTIVE_LEAF) {
          break;
        }

        child->share = calculateShare(child, slaveId);
        ++childIter;
      }

      std::sort(node->children.begin(),
                childIter,
                RPSDSFSorter::Node::compareRPSDSF);

      foreach (Node* child, node->children) {
        if (child->kind == Node::INTERNAL) {
          sortTree(child);
        } else if (child->kind == Node::INACTIVE_LEAF) {
          break;
        }
      }
    };

    sortTree(root);

    dirty = false;
  }

  std::priority_queue<std::pair<std::string, double>,
                          std::vector< std::pair<std::string, double> >,
                          Sorter::ComparePair> result;

  std::function<void (const Node*)> listClients =
      [&listClients, &result](const Node* node) {
    foreach (const Node* child, node->children) {
      switch (child->kind) {
        case Node::ACTIVE_LEAF:
        {
          std::pair<std::string, double> tmp(
              child->clientPath(), child->share);
          LOG(INFO) << "=== name :" << tmp.first << ", share: " << tmp.second;
          result.push(tmp);
          break;
        }
        case Node::INACTIVE_LEAF:
          return;

        case Node::INTERNAL:
          listClients(child);
          break;
      }
    }
  };

  listClients(root);

  return result;
}

double RPSDSFSorter::updateVirtualShare(
    const std::pair<std::string, double>& elem,
    const SlaveID& slaveId) const
{
  return calculateShare(find(elem.first), slaveId);
}

bool RPSDSFSorter::contains(const string& clientPath) const
{
  return find(clientPath) != nullptr;
}


int RPSDSFSorter::count() const
{
  return clients.size();
}

void RPSDSFSorter::printPerSlaveResources() const
{
  foreachpair (const SlaveID& slaveId,
               const Resources& resources,
               total_.resources) {
    LOG(INFO) << "---Slave: " << slaveId.value();
    Option<double> cpus = resources.cpus();
    if (!cpus.isNone()) {
        LOG(INFO) << "------ CPUs: " << cpus.get();
    } else {
        LOG(INFO) << "------ CPUs: None";
    }
    Option<Bytes> mem = resources.mem();
    if (!mem.isNone()) {
        LOG(INFO) << "------ Mem: " << mem.get().megabytes();
    } else {
        LOG(INFO) << "------ Mem: None";
    }
  }
}

double RPSDSFSorter::calculateShare(const Node* node,
                                   const SlaveID& slaveId) const
{
  double share = 0.0;

  // TODO(yuquanshan): currently only consider cpu and mem, may iterate
  // Resources in the future

  // Add the following if statement because slave wont be immediately
  // registered in frameworkSorter (registered only after this slave's
  // resources have been allocated under a role), so there will be a problem
  // in total_.resource.at(.) if we don't have .contains() check as follows.
  if (total_.resources.contains(slaveId)) {
    // first comes CPU
    Option<double> totalCpus = total_.resources.at(slaveId).cpus();

    double allocatedCpus = 0.0;
    for (auto childIter = root->children.begin();
        childIter != root->children.end(); childIter++) {
      Node* child = *childIter;
      if (child->allocation.resources.contains(slaveId) &&
          !child->allocation.resources.at(slaveId).cpus().isNone()) {
        allocatedCpus += child->allocation.resources.at(slaveId).cpus().get();
      }
    }

    if ((!fairnessExcludeResourceNames.isSome() ||
        (fairnessExcludeResourceNames.isSome() &&
        fairnessExcludeResourceNames->count("cpus") > 0)) &&
        !totalCpus.isNone() &&
        totalCpus.get() - allocatedCpus > 0 &&
        node->allocation.totals.contains("cpus")) {
      const double allocation = node->allocation.totals.at("cpus").value();
      share = std::max(share, allocation / (totalCpus.get() - allocatedCpus));
    }

    // second comes memory
    Option<Bytes> totalMem = total_.resources.at(slaveId).mem();

    double allocatedMem = 0.0;
    for (auto childIter = root->children.begin();
        childIter != root->children.end(); childIter++) {
      Node* child = *childIter;
      if (child->allocation.resources.contains(slaveId) &&
          !child->allocation.resources.at(slaveId).mem().isNone()) {
        allocatedMem += child->allocation.resources.at(slaveId).mem()
            .get().megabytes();
      }
    }

    if ((!fairnessExcludeResourceNames.isSome() ||
        (fairnessExcludeResourceNames.isSome() &&
        fairnessExcludeResourceNames->count("mem") > 0)) &&
        !totalMem.isNone() &&
        totalMem.get().megabytes() - allocatedMem > 0 &&
        node->allocation.totals.contains("mem")) {
      const double allocation = node->allocation.totals.at("mem").value();
      share = std::max(share,
                allocation / (totalMem.get().megabytes() - allocatedMem));
    }
  }
  return share / findWeight(node);
}


double RPSDSFSorter::findWeight(const Node* node) const
{
  Option<double> weight = weights.get(node->path);

  if (weight.isNone()) {
    return 1.0;
  }

  return weight.get();
}


RPSDSFSorter::Node* RPSDSFSorter::find(const string& clientPath) const
{
  Option<Node*> client_ = clients.get(clientPath);

  if (client_.isNone()) {
    return nullptr;
  }

  Node* client = client_.get();

  CHECK(client->isLeaf());

  return client;
}

} // namespace allocator {
} // namespace master {
} // namespace internal {
} // namespace mesos {
