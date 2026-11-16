import React from "react";
import { View, Text, StyleSheet } from "react-native";

export default function PnlScreen() {
  return (
    <View style={styles.container}>
      <Text style={styles.heading}>P&L Overview</Text>
      <Text style={styles.subtitle}>
        Your financial analytics will appear here.
      </Text>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: "#F7F9F8",
    padding: 24,
    paddingTop: 55,
  },
  heading: {
    fontSize: 28,
    fontWeight: "700",
    color: "#25332E",
  },
  subtitle: {
    color: "#7B8580",
    marginTop: 10,
  },
});