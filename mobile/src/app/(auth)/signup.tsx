import React, { useState } from "react";
import {
    View,
    Text,
    TextInput,
    TouchableOpacity,
    StyleSheet,
    KeyboardAvoidingView,
    Platform,
    Alert,
    ScrollView,
} from "react-native";
import { Link } from "expo-router";

export default function SignUpScreen() {
    const [name, setName] = useState("");
    const [email, setEmail] = useState("");
    const [password, setPassword] = useState("");
    const [confirmPassword, setConfirmPassword] = useState("");

    const handleSignUp = () => {
        if (!name.trim() || !email.trim() || !password || !confirmPassword) {
            Alert.alert("Missing details", "Please complete all fields.");
            return;
        }

        if (password.length < 8) {
            Alert.alert("Invalid password", "Password must be at least 8 characters.");
            return;
        }

        if (password !== confirmPassword) {
            Alert.alert("Password mismatch", "Your passwords do not match.");
            return;
        }

        Alert.alert(
            "Registration not connected",
            "The Sign Up screen is ready. We need to confirm your backend registration endpoint before enabling account creation."
        );
    };

    return (
        <KeyboardAvoidingView
            style={styles.container}
            behavior={Platform.OS === "ios" ? "padding" : undefined}
        >
            <ScrollView
                contentContainerStyle={styles.content}
                keyboardShouldPersistTaps="handled"
            >
                <Text style={styles.brand}>PHORMULA</Text>
                <Text style={styles.heading}>Create Account</Text>
                <Text style={styles.subtitle}>
                    Get started with your Phormula business dashboard.
                </Text>

                <Text style={styles.label}>Full Name</Text>
                <TextInput
                    style={styles.input}
                    placeholder="Enter your full name"
                    placeholderTextColor="#999"
                    value={name}
                    onChangeText={setName}
                />

                <Text style={styles.label}>Email Address</Text>
                <TextInput
                    style={styles.input}
                    placeholder="Enter your email"
                    placeholderTextColor="#999"
                    value={email}
                    onChangeText={setEmail}
                    keyboardType="email-address"
                    autoCapitalize="none"
                    autoCorrect={false}
                />

                <Text style={styles.label}>Password</Text>
                <TextInput
                    style={styles.input}
                    placeholder="Create a password"
                    placeholderTextColor="#999"
                    value={password}
                    onChangeText={setPassword}
                    secureTextEntry
                />

                <Text style={styles.label}>Confirm Password</Text>
                <TextInput
                    style={styles.input}
                    placeholder="Confirm your password"
                    placeholderTextColor="#999"
                    value={confirmPassword}
                    onChangeText={setConfirmPassword}
                    secureTextEntry
                />

                <TouchableOpacity style={styles.button} onPress={handleSignUp}>
                    <Text style={styles.buttonText}>Create Account</Text>
                </TouchableOpacity>

                <View style={styles.footer}>
                    <Text style={styles.footerText}>Already have an account? </Text>
                    <Link href="/(auth)/signin" style={styles.link}>
                        Sign In
                    </Link>
                </View>
            </ScrollView>
        </KeyboardAvoidingView>
    );
}

const styles = StyleSheet.create({
    container: {
        flex: 1,
        backgroundColor: "#F7F9F8",
    },
    content: {
        flexGrow: 1,
        justifyContent: "center",
        padding: 24,
        width: "100%",
        maxWidth: 488,
        alignSelf: "center",
    },
    brand: {
        color: "#5EA68E",
        fontSize: 24,
        fontWeight: "800",
        letterSpacing: 2,
        marginBottom: 28,
    },
    heading: {
        fontSize: 29,
        fontWeight: "700",
        color: "#25332E",
    },
    subtitle: {
        color: "#7B8580",
        fontSize: 14,
        marginTop: 8,
        marginBottom: 25,
    },
    label: {
        color: "#34423C",
        fontSize: 13,
        fontWeight: "600",
        marginBottom: 8,
    },
    input: {
        height: 50,
        borderWidth: 1,
        borderColor: "#DDE5E0",
        borderRadius: 10,
        paddingHorizontal: 14,
        backgroundColor: "#FFFFFF",
        color: "#26352E",
        marginBottom: 17,
        fontSize: 14,
    },
    button: {
        height: 52,
        backgroundColor: "#5EA68E",
        borderRadius: 10,
        alignItems: "center",
        justifyContent: "center",
        marginTop: 8,
    },
    buttonText: {
        color: "#FFFFFF",
        fontSize: 15,
        fontWeight: "700",
    },
    footer: {
        flexDirection: "row",
        justifyContent: "center",
        marginTop: 24,
    },
    footerText: {
        color: "#7B8580",
        fontSize: 14,
    },
    link: {
        color: "#5EA68E",
        fontWeight: "700",
        fontSize: 14,
    },
});