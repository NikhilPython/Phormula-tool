import { View, Text, StyleSheet, TouchableOpacity } from "react-native";
import { useAuth } from "../../services/auth/AuthContext";
import { useRouter } from "expo-router";

export default function AccountScreen() {
  const { user, signOut } = useAuth();
  const router = useRouter();

  const handleLogout = async () => {
    await signOut();
    router.replace("/(auth)/signin");
  };

  return (
    <View style={styles.container}>
      <Text style={styles.title}>Account</Text>
      <Text style={styles.subtitle}>{user?.member_name || "User"}</Text>
      
      <TouchableOpacity style={styles.logoutBtn} onPress={handleLogout}>
        <Text style={styles.logoutText}>Sign Out</Text>
      </TouchableOpacity>
    </View>
  );
}
const styles = StyleSheet.create({
  container: { flex: 1, justifyContent: "center", alignItems: "center", backgroundColor: "#F7F9F8" },
  title: { fontSize: 24, fontWeight: "bold", color: "#25332E", marginBottom: 8 },
  subtitle: { fontSize: 16, color: "#7B8580", marginBottom: 30 },
  logoutBtn: { padding: 15, borderRadius: 10, borderWidth: 1, borderColor: "#5EA68E", width: "80%", alignItems: "center" },
  logoutText: { color: "#5EA68E", fontWeight: "bold" },
});