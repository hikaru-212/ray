TEST_P(GcsClientTest, TestReregisterSameHeadNode) {
  auto head_node_info = GenNodeInfo(1);
  head_node_info->set_is_head_node(true);
  ASSERT_TRUE(RegisterNode(*head_node_info));

  // A retry after losing the registration reply must not treat this head as
  // a different head replacing the existing one.
  ASSERT_TRUE(RegisterNode(*head_node_info));

  const auto nodes = GetNodeInfoList();
  ASSERT_EQ(nodes.size(), 1);
  EXPECT_EQ(nodes[0].node_id(), head_node_info->node_id());
  EXPECT_EQ(nodes[0].state(), rpc::GcsNodeInfo::ALIVE);
}
