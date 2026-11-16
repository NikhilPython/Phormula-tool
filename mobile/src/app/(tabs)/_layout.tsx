import { Tabs, Redirect, useRouter } from "expo-router";
import { useAuth } from "../../services/auth/AuthContext";
import { ActivityIndicator, View, StyleSheet, TouchableOpacity } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";
import { Ionicons, MaterialIcons } from "@expo/vector-icons";
import { COLORS } from "../../constants/colors";

import {
  WalletIcon,
  AnalyticsIcon,
  PieChartIcon,
  ScanIcon,
  GridIcon,
} from "../../components/icons/Icons";

// Ionicons Map
const IONICONS: Record<string, keyof typeof Ionicons.glyphMap> = {
  pnl: "trending-up",
  account: "person-outline",
};

// 2. Material Icons Map (Added your requested icon here)
const MATERIAL_ICONS: Record<string, keyof typeof MaterialIcons.glyphMap> = {
  inventory: "inventory",
  // You can add more Material icons here
};

// Custom SVG Icons Map
const ICON_COMPONENTS: Record<string, React.ComponentType<{ color?: string; size?: number }>> = {
  dashboard: GridIcon, // Note: changed this to WalletIcon to match your image, change back to GridIcon if you prefer
  // pnl: AnalyticsIcon,
  // inventory: PieChartIcon,
  scan: ScanIcon,
  // account is handled by EXPO_ICONS
};

function CustomTabBar({ state }: any) {
  const insets = useSafeAreaInsets();
  const router = useRouter();

  return (
    <View
      pointerEvents="box-none"
      style={[styles.wrapper, { bottom: Math.max(insets.bottom, 16) }]}
    >
      <View style={styles.tabBarContainer}>
        {state.routes.map((route: any, index: number) => {
          const isFocused = state.index === index;

          const onPress = () => {
            if (!isFocused) {
              router.navigate(`/(tabs)/${route.name}` as any);
            }
          };

          // 1. Grab the icon definitions
          const SvgIcon = ICON_COMPONENTS[route.name];
          const IoniconsName = IONICONS[route.name];
          const MaterialIconName = MATERIAL_ICONS[route.name];

          // 2. Shared styling props
          const iconColor = isFocused ? COLORS.lightYellow : COLORS.charcoal;
          const iconSize = 20;

          // 3. Determine which icon to render
          let renderIcon;
          if (SvgIcon) {
            renderIcon = <SvgIcon size={iconSize} color={iconColor} />;
          } else if (IoniconsName) {
            renderIcon = <Ionicons name={IoniconsName} size={iconSize} color={iconColor} />;
          } else if (MaterialIconName) {
            renderIcon = <MaterialIcons name={MaterialIconName} size={iconSize} color={iconColor} />;
          } else {
            renderIcon = <GridIcon size={iconSize} color={iconColor} />; // Fallback
          }

          return (
            <TouchableOpacity
              key={route.key}
              accessibilityRole="button"
              accessibilityLabel={route.name}
              accessibilityState={isFocused ? { selected: true } : {}}
              onPress={onPress}
              activeOpacity={0.7}
              style={[styles.iconContainer, isFocused && styles.iconContainerActive]}
            >
              {/* 4. Render the dynamic icon */}
              {renderIcon}
            </TouchableOpacity>
          );
        })}
      </View>
    </View>
  );
}

export default function TabsLayout() {
  const { token, loading } = useAuth();

  if (loading) {
    return (
      <View style={{ flex: 1, justifyContent: "center", alignItems: "center" }}>
        <ActivityIndicator color="#5EA68E" size="large" />
      </View>
    );
  }

  if (!token) {
    return <Redirect href="/(auth)/signin" />;
  }

  return (
    <Tabs
      tabBar={(props) => <CustomTabBar {...props} />}
      screenOptions={{
        headerShown: false,
        tabBarShowLabel: false,
      }}
    >
      <Tabs.Screen name="dashboard" options={{ title: "Dashboard" }} />
      <Tabs.Screen name="pnl" options={{ title: "P&L" }} />
      <Tabs.Screen name="inventory" options={{ title: "Inventory" }} />
      <Tabs.Screen name="scan" options={{ title: "Scan" }} />
      <Tabs.Screen name="account" options={{ title: "Account" }} />
    </Tabs>
  );
}

const styles = StyleSheet.create({
  wrapper: {
    position: "absolute",
    left: 0,
    right: 0,
    alignItems: "center",
  },
  tabBarContainer: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    padding: 6,
    borderRadius: 999,
    borderWidth: StyleSheet.hairlineWidth,
    backgroundColor: COLORS.bgPill,
    borderColor: COLORS.border,
    shadowColor: COLORS.shadow,
    shadowOffset: { width: 0, height: 6 },
    shadowOpacity: 0.1,
    shadowRadius: 14,
    elevation: 8,
  },
  iconContainer: {
    width: 44,
    height: 44,
    borderRadius: 22,
    justifyContent: "center",
    alignItems: "center",
  },
  iconContainerActive: {
    backgroundColor: COLORS.green,
  },
});