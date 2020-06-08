package com.example.inf551_project;

import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.ListView;
import android.widget.SearchView;
import android.widget.Spinner;
import android.widget.Toast;

import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;
import com.google.firebase.database.core.utilities.Tree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class MainActivity extends AppCompatActivity implements AdapterView.OnItemSelectedListener, AdapterView.OnItemClickListener {
    ArrayAdapter<CharSequence> adapter;
    SearchView sv;
    Spinner spinner;
    FirebaseDatabase database;
    DatabaseReference myRef;
    DataSnapshot ds;
    ListView lv;
    ArrayList<CharSequence> arrayList;
    ArrayAdapter<CharSequence> arrayAdapter;
    String spinnerValue;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        database = FirebaseDatabase.getInstance();
        myRef = database.getReference().child("Project");

        // Read from the database
        myRef.addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                // This method is called once with the initial value and again
                // whenever data at this location is updated.
                ds = dataSnapshot;
            }

            @Override
            public void onCancelled(DatabaseError error) {
                // Failed to read value
                Log.w("yooo", "Failed to read value.", error.toException());
            }
        });

        sv = findViewById(R.id.search);
        lv = findViewById(R.id.result_list);
        lv.setOnItemClickListener(this);
        arrayList = new ArrayList<>();
        arrayAdapter = new ArrayAdapter<>(MainActivity.this, android.R.layout.simple_list_item_1, arrayList);
        lv.setAdapter(arrayAdapter);

        sv.setOnQueryTextListener(new SearchView.OnQueryTextListener() {
            @Override
            public boolean onQueryTextSubmit(String str) {
                arrayList.clear();
                arrayAdapter.notifyDataSetChanged();
                String inverted_table = "";
                if(spinnerValue.equals("World Cup")){
                    inverted_table = "WCinverted";
                }
                else if(spinnerValue.equals("NBA")){
                    inverted_table = "NBAinverted";
                }
                else if(spinnerValue.equals("World")){
                    inverted_table = "Cinverted";
                }
                String[] arr = str.split(" ");
                TreeMap<Double, String> map = new TreeMap<>();
                HashMap<String, String> id_map = new HashMap<>();
                HashMap<String, String> table_map = new HashMap<>();
                for(int j = 0; j < arr.length; j++){
                    String s = arr[j];
                    if(ds != null && ds.getValue() != null) {
                        DataSnapshot temp_ds = ds.child(inverted_table).child(s.toLowerCase());
                        Iterable<DataSnapshot> allData = temp_ds.getChildren();
                        ArrayList<String> table_list = new ArrayList<>();
                        ArrayList<String> id_list = new ArrayList<>();

                        for(DataSnapshot currDS: allData){
                            table_list.add(currDS.child("Table").getValue().toString());
                            id_list.add(currDS.child("ID").getValue().toString());
                        }

                        for(int i = 0; i < table_list.size(); i++){
                            double scores = 0.0;
                            String table = table_list.get(i);
                            String id = id_list.get(i);
                            Log.d("querysubmit","Table: " + table + " ID: " + id);
                            Log.d("querysubmit", ds.child(table).child(id).getValue().toString());
                            DataSnapshot result_tuple = ds.child(table).child(id);
                            Iterable<DataSnapshot> tableData = result_tuple.getChildren();
                            for(DataSnapshot currDS: tableData){
                                Log.d("resulttuple", "Key: " + currDS.getKey().toString() + " Value: " + currDS.getValue().toString());
                            }
                            if(result_tuple.getValue().toString().toLowerCase().contains(str.toLowerCase())){
                                scores -= 20.0;
                            }
                            if(result_tuple.getValue().toString().toLowerCase().contains(s.toLowerCase())){
                                scores -= 1.0;
                            }
                            while(map.containsKey(scores)){
                                scores -= 0.001;
                            }
                            if(!map.containsValue(result_tuple.getValue().toString())){
                                map.put(scores,result_tuple.getValue().toString());
                                id_map.put(result_tuple.getValue().toString(),id);
                                table_map.put(result_tuple.getValue().toString(),table);
                            }

                            /*if(!arrayList.contains(result_tuple.getValue().toString())){
                                arrayList.add(result_tuple.getValue().toString());
                                arrayAdapter.notifyDataSetChanged();
                            }*/
                        }
                        for(Map.Entry<Double,String> entry: map.entrySet()){
                            if(!arrayList.contains("Table: " + table_map.get(entry.getValue()) + "\n" + entry.getValue())) {
                                arrayList.add("Table: " + table_map.get(entry.getValue()) + "\n" + entry.getValue());
                                arrayAdapter.notifyDataSetChanged();
                            }
                        }
                    }
                }
                return false;
            }

            @Override
            public boolean onQueryTextChange(String s) {
                adapter.getFilter().filter(s);
                Log.d("querysubmit", s);
                return false;
            }
        });

        spinner = findViewById(R.id.spinner);
        adapter = ArrayAdapter.createFromResource(this, R.array.databases, android.R.layout.simple_spinner_item);
        adapter.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item);
        spinner.setAdapter(adapter);
        spinner.setOnItemSelectedListener(this);

    }

    @Override
    public void onItemSelected(AdapterView<?> parent, View view, int pos, long id) {
        String text = parent.getItemAtPosition(pos).toString();
        Toast.makeText(parent.getContext(), text, Toast.LENGTH_SHORT).show();
        spinnerValue = text;
        spinner.setSelection(pos, false);
        Log.d("spinner", text);
    }

    @Override
    public void onNothingSelected(AdapterView<?> adapterView) {

    }

    @Override
    public void onItemClick(AdapterView<?> adapterView, View view, int position, long id) {
        String selected = (String) adapterView.getItemAtPosition(position);
        arrayList.clear();
        arrayAdapter.notifyDataSetChanged();
        if(selected.contains("Table: nbaplayers")){
            String temp = selected.substring(selected.indexOf("TEAM_ABB"));
            String team = temp.substring(temp.indexOf("=") + 1, temp.indexOf(","));
            Iterable<DataSnapshot> temp_it = ds.child("nbateam").getChildren();
            for(DataSnapshot curr: temp_it) {
                if(curr.child("TEAM_ABBREVIATION").getValue().toString().equals(team)){
                    arrayList.add("Table: nbateam" + "\n" + curr.getValue().toString());
                    arrayAdapter.notifyDataSetChanged();
                }
            }
        }
        else if(selected.contains("Table: nbasalary")){
            String temp = selected.substring(selected.indexOf("TEAM_ABB"));
            String team = temp.substring(temp.indexOf("=") + 1, temp.indexOf(","));
            Iterable<DataSnapshot> temp_it = ds.child("nbateam").getChildren();
            for(DataSnapshot curr: temp_it) {
                if(curr.child("TEAM_ABBREVIATION").getValue().toString().equals(team)){
                    arrayList.add("Table: nbateam" + "\n" + curr.getValue().toString());
                    arrayAdapter.notifyDataSetChanged();
                }
            }
        }
        else if(selected.contains("Table: nbateam")){
            String temp = selected.substring(selected.indexOf("TEAM_ABB"));
            String team = temp.substring(temp.indexOf("=") + 1, temp.indexOf(","));
            Iterable<DataSnapshot> temp_it = ds.child("nbaplayers").getChildren();
            for(DataSnapshot curr: temp_it) {
                if(curr.child("TEAM_ABBREVIATION").getValue().toString().equals(team)){
                    arrayList.add("Table: nbaplayers" + "\n" + curr.getValue().toString());
                    arrayAdapter.notifyDataSetChanged();
                }
            }
            Iterable<DataSnapshot> temp_it2 = ds.child("nbasalary").getChildren();
            for(DataSnapshot curr: temp_it2) {
                if(curr.child("TEAM_ABBREVIATION").getValue().toString().equals(team)){
                    arrayList.add("Table: nbasalary" + "\n" + curr.getValue().toString());
                    arrayAdapter.notifyDataSetChanged();
                }
            }
        }
        else if(selected.contains("Table: city")){
            String temp = selected.substring(selected.indexOf("CountryCode"));
            String country = temp.substring(temp.indexOf("=") + 1, temp.indexOf(","));
            Iterable<DataSnapshot> temp_it = ds.child("country").getChildren();
            for(DataSnapshot curr: temp_it) {
                if(curr.child("CountryCode").getValue().toString().equals(country)){
                    arrayList.add("Table: country" + "\n" + curr.getValue().toString());
                    arrayAdapter.notifyDataSetChanged();
                }
            }
        }
        else if(selected.contains("Table: countrylanguage")){
            String temp = selected.substring(selected.indexOf("CountryCode"));
            String country = temp.substring(temp.indexOf("=") + 1, temp.indexOf("}"));
            Iterable<DataSnapshot> temp_it = ds.child("country").getChildren();
            for(DataSnapshot curr: temp_it) {
                if(curr.child("CountryCode").getValue().toString().equals(country)){
                    arrayList.add("Table: country" + "\n" + curr.getValue().toString());
                    arrayAdapter.notifyDataSetChanged();
                }
            }
        }
        else if(selected.contains("Table: country")){
            String temp = selected.substring(selected.indexOf("CountryCode"));
            String country = temp.substring(temp.indexOf("=") + 1, temp.indexOf("}"));
            Iterable<DataSnapshot> temp_it = ds.child("city").getChildren();
            for(DataSnapshot curr: temp_it) {
                if(curr.child("CountryCode").getValue().toString().equals(country)){
                    arrayList.add("Table: city" + "\n" + curr.getValue().toString());
                    arrayAdapter.notifyDataSetChanged();
                }
            }
            Iterable<DataSnapshot> temp_it2 = ds.child("countrylanguage").getChildren();
            for(DataSnapshot curr: temp_it2) {
                if(curr.child("CountryCode").getValue().toString().equals(country)){
                    arrayList.add("Table: countrylanguage" + "\n" + curr.getValue().toString());
                    arrayAdapter.notifyDataSetChanged();
                }
            }
        }
        else if(selected.contains("Table: worldcupplayers")){
            String temp = selected.substring(selected.indexOf("MatchID"));
            String match = temp.substring(temp.indexOf("=") + 1, temp.indexOf(","));
            Iterable<DataSnapshot> temp_it = ds.child("worldcupmatches").getChildren();
            for(DataSnapshot curr: temp_it) {
                if(curr.child("MatchID").getValue().toString().equals(match)){
                    arrayList.add("Table: worldcupmatches" + "\n" + curr.getValue().toString());
                    arrayAdapter.notifyDataSetChanged();
                }
            }
        }
        else if(selected.contains("Table: worldcupmatches")){
            String temp = selected.substring(selected.indexOf("Year"));
            String year = temp.substring(temp.indexOf("=") + 1, temp.indexOf(","));
            Iterable<DataSnapshot> temp_it = ds.child("worldcups").getChildren();
            for(DataSnapshot curr: temp_it) {
                if(curr.child("Year").getValue().toString().equals(year)){
                    arrayList.add("Table: worldcups" + "\n" + curr.getValue().toString());
                    arrayAdapter.notifyDataSetChanged();
                }
            }
        }
        else if(selected.contains("Table: worldcups")){
            String temp = selected.substring(selected.indexOf("Year"));
            String year = temp.substring(temp.indexOf("=") + 1, temp.indexOf(","));
            Iterable<DataSnapshot> temp_it = ds.child("worldcupmatches").getChildren();
            for(DataSnapshot curr: temp_it) {
                if(curr.child("Year").getValue().toString().equals(year)){
                    arrayList.add("Table: worldcupmatches" + "\n" + curr.getValue().toString());
                    arrayAdapter.notifyDataSetChanged();
                }
            }
        }
    }
}
