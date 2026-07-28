"""In-memory Kafka surface: topics, ACLs, Direct Partition Assignment."""

from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple

from .errors import DEPENDENCY_UNAVAILABLE, INVALID_PARAMS, McpError


class InMemoryKafka:
    """Minimal Kafka-like backend for the reference MCP server."""

    def __init__(self) -> None:
        self.topics: Dict[str, Dict[str, Any]] = {}
        self.groups: Dict[str, Dict[str, Any]] = {}
        self.acls: List[Dict[str, Any]] = []
        self.principal_acls: Dict[str, List[Tuple[str, str]]] = {}
        self.acl_enforcement: bool = False
        self.rebalances: int = 0
        self.cluster_id: str = "ref-cluster-" + uuid.uuid4().hex[:8]
        self._dependency_failure: bool = False
        self._failed_modules: Set[str] = set()

    # --- test hooks ---
    def _inject_dependency_failure(self, enabled: bool = True) -> None:
        self._dependency_failure = enabled

    def _fail_module(self, module: Optional[str], enabled: bool = True) -> None:
        if module is None:
            self._failed_modules.clear()
            return
        if enabled:
            self._failed_modules.add(module)
        else:
            self._failed_modules.discard(module)

    def check_dependency(self, module: Optional[str] = None) -> None:
        if self._dependency_failure:
            raise McpError(DEPENDENCY_UNAVAILABLE, "dependency unavailable")
        if module and module in self._failed_modules:
            raise McpError(
                DEPENDENCY_UNAVAILABLE,
                f"dependency unavailable for module {module}",
            )

    # --- ACLs ---
    def set_principal_acl(self, principal: str, operation: str, prefix: str) -> None:
        self.acl_enforcement = True
        self.principal_acls.setdefault(principal, []).append((operation.upper(), prefix))

    def authorize(self, principal: str, operation: str, name: str) -> bool:
        if not self.acl_enforcement:
            return True
        rules = self.principal_acls.get(principal, [])
        op = (operation or "").upper()
        for rule_op, prefix in rules:
            if rule_op in (op, "ALL") and (name or "").startswith(prefix):
                return True
        return False

    # --- topics ---
    def create_topic(
        self,
        name: str,
        partitions: int = 1,
        replication_factor: int = 1,
        config: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        if name in self.topics:
            raise McpError(
                INVALID_PARAMS,
                "TopicExists",
                data={"error": "TopicExists", "topic": name},
            )
        self.topics[name] = {
            "config": dict(config or {}),
            "partitions": int(partitions),
            "replication_factor": int(replication_factor),
            "log": {i: [] for i in range(int(partitions))},
        }
        return {
            "name": name,
            "partitions": int(partitions),
            "replicationFactor": int(replication_factor),
        }

    def delete_topic(self, name: str) -> Dict[str, Any]:
        if name not in self.topics:
            raise McpError(
                INVALID_PARAMS,
                "UnknownTopic",
                data={"error": "UnknownTopic", "topic": name},
            )
        del self.topics[name]
        return {"deleted": name}

    def list_topics(self) -> Dict[str, Any]:
        return {"topics": sorted(self.topics.keys())}

    def describe_topic(self, name: str) -> Dict[str, Any]:
        t = self.topics.get(name)
        if not t:
            raise McpError(
                INVALID_PARAMS,
                "UnknownTopic",
                data={"error": "UnknownTopic", "topic": name},
            )
        return {
            "name": name,
            "partitions": t["partitions"],
            "replicationFactor": t["replication_factor"],
            "config": dict(t["config"]),
        }

    def alter_topic_config(self, name: str, config: Dict[str, str]) -> Dict[str, Any]:
        t = self.topics.get(name)
        if not t:
            raise McpError(
                INVALID_PARAMS,
                "UnknownTopic",
                data={"error": "UnknownTopic", "topic": name},
            )
        t["config"].update(config or {})
        return {"name": name, "config": dict(t["config"])}

    def produce(
        self,
        topic: str,
        value: str,
        key: Optional[str] = None,
        partition: int = 0,
    ) -> Dict[str, Any]:
        t = self.topics.get(topic)
        if not t:
            raise McpError(
                INVALID_PARAMS,
                "UnknownTopic",
                data={"error": "UnknownTopic", "topic": topic},
            )
        parts = t["partitions"]
        p = int(partition) % parts
        offset = len(t["log"][p])
        rec = {
            "offset": offset,
            "partition": p,
            "key": key,
            "value": value,
            "timestamp": time.time(),
        }
        t["log"][p].append(rec)
        return {"topic": topic, "partition": p, "offset": offset}

    def consume(
        self,
        topic: str,
        max_messages: int = 10,
        from_beginning: bool = True,
        group_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        t = self.topics.get(topic)
        if not t:
            raise McpError(
                INVALID_PARAMS,
                "UnknownTopic",
                data={"error": "UnknownTopic", "topic": topic},
            )
        records: List[Dict[str, Any]] = []
        for p in range(t["partitions"]):
            log = t["log"][p]
            if group_id:
                g = self.groups.setdefault(
                    group_id,
                    {"offsets": {}, "topics": set()},
                )
                if topic not in g["topics"]:
                    g["topics"].add(topic)
                    self.rebalances += 1
                committed = g["offsets"].get(f"{topic}:{p}", 0 if from_beginning else len(log))
                start = 0 if from_beginning and f"{topic}:{p}" not in g["offsets"] else committed
                chunk = log[start:]
                for rec in chunk:
                    if len(records) >= max_messages:
                        break
                    records.append(
                        {
                            "topic": topic,
                            "partition": p,
                            "offset": rec["offset"],
                            "key": rec.get("key"),
                            "value": rec.get("value"),
                        }
                    )
                if records:
                    last = records[-1]
                    if last["partition"] == p:
                        g["offsets"][f"{topic}:{p}"] = last["offset"] + 1
                if len(records) >= max_messages:
                    break
            else:
                # Direct Partition Assignment: no group, no rebalance
                chunk = log if from_beginning else log[-max_messages:]
                for rec in chunk:
                    if len(records) >= max_messages:
                        break
                    records.append(
                        {
                            "topic": topic,
                            "partition": p,
                            "offset": rec["offset"],
                            "key": rec.get("key"),
                            "value": rec.get("value"),
                        }
                    )
                if len(records) >= max_messages:
                    break

        if group_id is None:
            return {
                "assignment": "direct",
                "groupId": None,
                "records": records,
            }
        return {
            "assignment": "group",
            "groupId": group_id,
            "records": records,
        }

    def list_groups(self) -> Dict[str, Any]:
        return {"groups": sorted(self.groups.keys())}

    def describe_group(self, group_id: str) -> Dict[str, Any]:
        g = self.groups.get(group_id)
        if not g:
            return {"groupId": group_id, "offsets": {}, "state": "Empty"}
        return {
            "groupId": group_id,
            "offsets": dict(g["offsets"]),
            "state": "Stable",
            "topics": sorted(g.get("topics", [])),
        }

    def group_lag(self, group_id: str, topic: str) -> Dict[str, Any]:
        t = self.topics.get(topic)
        if not t:
            raise McpError(
                INVALID_PARAMS,
                "UnknownTopic",
                data={"error": "UnknownTopic", "topic": topic},
            )
        g = self.groups.get(group_id, {"offsets": {}})
        lags = []
        total = 0
        for p in range(t["partitions"]):
            end = len(t["log"][p])
            committed = g["offsets"].get(f"{topic}:{p}", 0)
            lag = max(0, end - committed)
            total += lag
            lags.append({"partition": p, "endOffset": end, "committed": committed, "lag": lag})
        return {"groupId": group_id, "topic": topic, "lag": total, "partitions": lags}

    def create_acls(self, bindings: List[Dict[str, Any]]) -> Dict[str, Any]:
        for b in bindings or []:
            self.acls.append(dict(b))
        return {"created": len(bindings or []), "acls": list(self.acls)}

    def list_acls(self) -> Dict[str, Any]:
        return {"acls": list(self.acls)}

    def describe_cluster(self) -> Dict[str, Any]:
        return {
            "clusterId": self.cluster_id,
            "brokers": [
                {"id": 1, "host": "localhost", "port": 9092},
            ],
            "controller": 1,
        }
